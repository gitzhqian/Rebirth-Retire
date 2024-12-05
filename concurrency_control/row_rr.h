//
// Created by root on 2024/7/18.
//

#ifndef DBX1000_ROW_REBIRTH_RETIRE_H
#define DBX1000_ROW_REBIRTH_RETIRE_H

#pragma once

#include <stack>
#include <chrono>
#include <thread>
#include "row.h"
#include "txn.h"
#include "global.h"
#include "row_lock.h"
#include "map"

class table_t;
class Catalog;
class txn_man;

#if CC_ALG == REBIRTH_RETIRE

#define INF UINT64_MAX

struct Version;
struct HReader {
    txn_man* cur_reader;
    Version* prev;
    HReader* next;
    HReader(txn_man * read ): cur_reader(read), next(nullptr) {};
};

struct Version {
    ts_t begin_ts;
    ts_t end_ts;
    access_t type;
//    volatile ts_t *dynamic_txn_ts;  //pointing to the created transaction's ts
    HReader *read_queue;
    Version* prev;
    Version* next;
    txn_man* retire;      // the txn_man of the uncommitted txn which updates the tuple version
    row_t* data;

    Version(txn_man * txn):  begin_ts(INF), end_ts(INF),retire(txn),read_queue(nullptr),type(WR) {};

    void init(){
        this->begin_ts = INF;
        this->end_ts = INF;
        this->read_queue = nullptr;
        this->retire = nullptr;
        this->type = XP;
//        this->dynamic_txn_ts = new ts_t(0);
        this->prev = nullptr;
    }
};


class Row_rr {
public:
    void init(row_t *row);
    RC access(txn_man *txn, TsType type, Access *access);
    RC active_retire(LockEntry * entry);
    bool bring_next(txn_man * txn);

    volatile bool blatch;
    Version *version_header;              // version header of a row's version chain (N2O)
//    std::map<uint64_t, txn_man *> *wait_list;
    LockEntry * waiters_head;
    LockEntry * waiters_tail;
    volatile LockEntry * owner;
//    volatile txn_man * owner;
    UInt32 waiter_cnt;
    UInt32 retired_cnt;

    inline LockEntry *  get_entry(Access * access) {
        LockEntry * entry = access->lock_entry;
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
        entry->has_write = false;
        return entry;
    }

    inline bool bring_out_waiter(LockEntry * entry, txn_man * txn) {
//        printf("before waiter_cnt:%u, \n", waiter_cnt);
        LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
        if (entry->txn != nullptr){
            entry->txn->lock_ready = true;
        }
//        printf("after waiter_cnt:%u, \n", waiter_cnt);

        if (txn == entry->txn) {
            return true;
        }
        return false;
    };


    // push txn entry into waiter list, ordered by timestamps, ascending
    inline void add_to_waiters(ts_t ts, LockEntry * to_insert) {
        LockEntry * en = waiters_head;
        uint32_t traverse_sz = 0;
        bool find = false;
        while (en != NULL) {
            if (traverse_sz > waiter_cnt) {
                break;
            }
            if (ts < en->txn->get_ts()){
                find = true;
                break;
            }
            traverse_sz++;
            en = en->next;
        }
        if (find) {
            LIST_INSERT_BEFORE(en, to_insert);
            if (en == waiters_head){
                waiters_head = to_insert;
            }
        } else {
            LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert);
        }

        to_insert->status = LOCK_WAITER;
        to_insert->txn->lock_ready = false;
        waiter_cnt++;
        assert(ts != 0);
    };

    inline LockEntry * find_write_in_waiter(ts_t ts) {
        LockEntry * read = nullptr;
        LockEntry * en = waiters_head;
        for (UInt32 i = 0; i < retired_cnt; i++) {
            if ((en->type == LOCK_EX) && (en->txn != nullptr) && (en->txn->get_ts() < ts)) {
                if (en->txn->status != ABORTED ) {
                    read = en;
                    break;
                }
            }
            en = en->next;
        }

        return read;
    }

#if LATCH == LH_SPINLOCK
    pthread_spinlock_t * spinlock_row;
#else
    mcslock * latch_row;
#endif

    void  lock_row(txn_man * txn) const {
        if (likely(g_thread_cnt > 1)) {
#if LATCH == LH_SPINLOCK
            pthread_spin_lock(spinlock_row);
#else
            latch_row->acquire(txn->mcs_node);
#endif
        }
    };

    void  unlock_row(txn_man * txn) const {
        if (likely(g_thread_cnt > 1)) {
#if LATCH == LH_SPINLOCK
            pthread_spin_unlock(spinlock_row);
#else
            latch_row->release(txn->mcs_node);
#endif
        }
    };

    // check priorities, timestamps
    inline static bool a_higher_than_b(ts_t a, ts_t b) {
        return a < b;
    };

    inline static int assign_ts(ts_t ts, txn_man *txn) {
        if (ts == 0) {
            ts = txn->set_next_ts();
            // if fail to assign, reload
            if (ts == 0) {
                ts = txn->get_ts();
            }
        }
        return ts;
    };

    inline static int reassign_ts(txn_man *txn) {
        ts_t ts = 0;
        ts = txn->set_next_ts();
        // if fail to assign, reload
        if (ts == 0) {
            ts = txn->get_ts();
        }

        return ts;
    };

    // GC
    inline void remove_tombstones() {
        if (owner != nullptr && (owner->access->tuple_version->type == AT || (owner->txn != nullptr && owner->txn->status == ABORTED))){
            owner = nullptr;
        }
        while (version_header) {
            if (version_header->type == XP || (version_header->type == WR && version_header->retire->status != ABORTED)){
                break;
            }
            if (version_header->type == AT || (version_header->retire != nullptr && version_header->retire->status == ABORTED)) {
                if (owner == nullptr){
                    version_header = version_header->next;
                    version_header->prev = nullptr;
                }else{
                    owner->access->tuple_version->next = version_header->next;
                    version_header = version_header->next;
                    version_header->prev = nullptr;
                }
            }
        }
    }

    bool wound_rebirth(ts_t ts, txn_man *curr_txn, TsType type) {
        //1. check the conflicts
        // find txns whose timestamp priority lower than me
        auto lower_than_me = std::vector<Version *>();
        auto wound_header = version_header;
        while (wound_header) {
            if (wound_header->retire == nullptr && wound_header->type == XP) break;
            if (wound_header->type == AT) {
                wound_header = wound_header->next;
                continue;
            }

            auto write_txn_ = wound_header->retire;
            if (write_txn_ != nullptr  ) {
                if (a_higher_than_b(curr_txn->get_ts(), write_txn_->get_ts())) {
                    lower_than_me.push_back(wound_header);
                }
            }

            wound_header = wound_header->next;
        }

        if (owner != nullptr && owner->status == LOCK_OWNER ) {
            if (owner->txn != nullptr) {
                auto own_ts = owner->txn->get_ts();
                if (own_ts == 0 || a_higher_than_b(curr_txn->get_ts(), own_ts)) {
                    lower_than_me.push_back(owner->access->tuple_version);
                }
            }
        }

        if (lower_than_me.empty()) return false;

#if REBIRTH
        retry:
        uint64_t starttimeRB = get_sys_clock();
        if (curr_txn->status == ABORTED || curr_txn->children.empty()) return false;
        auto *adjacencyList = new std::unordered_map<uint64_t, std::set<txn_man*> *>(); // to<-from
        auto ret = curr_txn->buildGraph(adjacencyList, curr_txn);
        if (!ret) return false;
        // thread id, timestamp, txn version
        auto *sortedOrder = new std::vector<std::pair<uint64_t, std::pair<uint64_t , uint64_t>>>();
        auto ret1 = curr_txn->topologicalSort(adjacencyList, sortedOrder, nullptr);
        if (ret1){
            curr_txn->set_abort();
#if PF_CS
            INC_STATS(curr_txn->get_thd_id(), blind_kill_count, 1);
            INC_STATS(curr_txn->get_thd_id(), time_rebirth,  get_sys_clock() - starttimeRB);
#endif

            return true;
        }

        auto size_dep = lower_than_me.size();
        auto size_sort = sortedOrder->size();
        bool find = false;
        int find_idx = 0;
        // wound the txns whose timestamp larger than me
        if (size_dep > 0){
            for (int i = size_dep - 1; i >= 0; --i){
                auto dep_txn_o=lower_than_me[i]->retire;
                if (dep_txn_o != nullptr) {
                    if (find){
                        curr_txn->wound_txn(dep_txn_o);
#if PF_CS
                        INC_STATS(curr_txn->get_thd_id(), find_circle_abort_depent, 1);
#endif
                    }else{
                        for (int j = 0; j < size_sort; ++j) {
                            if (sortedOrder->at(j).first == dep_txn_o->get_thd_id()){
                                curr_txn->wound_txn(dep_txn_o);
#if PF_CS
                                INC_STATS(curr_txn->get_thd_id(), find_circle_abort_depent, 1);
#endif
                                find = true;
                                find_idx = i;
                            }
                        }
                    }
                }
            }
        }


#if NEXT_TS
        if (size_dep > 0 && size_sort > 1){
            //option 1: the next TS assigned
            uint64_t next_ts = 0;
            for (auto it = sortedOrder->begin(); it != sortedOrder->end(); ++it) {
                auto depent_txn_thd = it->first;
                auto depent_txn = glob_manager->get_txn_man(depent_txn_thd);
                if (depent_txn != nullptr){
                    next_ts = glob_manager->get_ts(depent_txn_thd);
                    depent_txn->set_ts(next_ts);
                }
            }
        }
#else
        // rebirth the txns in my children
        if (size_dep > 0 && size_sort > 1){
            //option 2: just larger than all the conflict txns
            auto max_ts = ts;
            auto dep_txn_o = lower_than_me[find_idx]->retire;
            max_ts = std::max(max_ts, dep_txn_o->get_ts());
            auto readers = wound_header->read_queue;
            HReader *read_ = nullptr;
            if (readers != nullptr) {
                read_ = readers;
                while (true) {
                    if (read_->next == nullptr) break;
                    auto read_txn_ = read_->cur_reader;
                    if (read_txn_ != nullptr && read_txn_->status != ABORTED) {
                        max_ts = std::max(max_ts, read_txn_->get_ts());
                    }
                    read_ = read_->next;
                }
            }

            uint64_t defer_ts = curr_txn->increment_high48(max_ts);
            for (auto it = sortedOrder->begin(); it != sortedOrder->end(); ++it) {
                auto depent_txn_thd_id = it->first;
                auto depent_txn = glob_manager->get_txn_man(depent_txn_thd_id);
                if (depent_txn != nullptr && depent_txn->status != ABORTED){
                    if (depent_txn->timestamp_v.load() != it->second.second) {
                        goto retry;
                    }
                    if (depent_txn->get_ts() < defer_ts){
                        defer_ts++;
                        depent_txn->set_ts(defer_ts);
                        depent_txn->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }
#endif

#if PF_CS
        INC_STATS(curr_txn->get_thd_id(), time_rebirth,  get_sys_clock() - starttimeRB);
#endif

        if (find){
            return true;
        }
        return false;
#else
        auto size_dep = lower_than_me.size();
        for (int i = 0; i < size_dep; ++i) {
            auto version_o = lower_than_me[i];
            version_o->type = AT;
            auto dep_txn_o = version_o->retire;
            if (dep_txn_o != nullptr) {
                dep_txn_o->set_abort();
                dep_txn_o->lock_abort = true;
#if PF_CS
                INC_STATS(curr_txn->get_thd_id(), find_circle_abort_depent, 1);
#endif
            }
        }

        return false;
#endif

    }


private:

//    Version * _write_history; // circular buffer, convert it to the thread class

};

#endif


#endif

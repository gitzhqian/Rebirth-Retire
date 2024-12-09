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

struct RRLockEntry {
    txn_man * txn;
    Access * access;
    lock_t type;
    bool has_write;
    lock_status status;
    RRLockEntry(txn_man * t, Access * a): txn(t), access(a), type(LOCK_NONE),
                                        status(LOCK_DROPPED) {};
};


class Row_rr {
public:
    void init(row_t *row);
    RC access(txn_man *txn, TsType type, Access *access);
    RC active_retire(RRLockEntry * entry);
    bool bring_next(txn_man * txn, txn_man * curr);

    volatile bool blatch;
    Version *version_header;              // version header of a row's version chain (N2O)
//    std::map<uint64_t, txn_man *> *wait_list;
//    LockEntry * waiters_head;
//    LockEntry * waiters_tail;
    std::list<RRLockEntry *> *entry_list;
    RRLockEntry * owner;
//    volatile txn_man * owner;
    UInt32 waiter_cnt;
    UInt32 retired_cnt;

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

    inline RRLockEntry *  get_entry(Access * access) {
        RRLockEntry * entry = access->lock_entry;
        entry->status = LOCK_DROPPED;
        entry->has_write = false;
        entry->type = LOCK_NONE;
        return entry;
    }

    inline void list_rm(RRLockEntry* entry) {
        if (entry == nullptr) {
            return;  // 如果传入的 entry 为 nullptr，直接返回
        }

        for (auto it = entry_list->begin(); it != entry_list->end(); ) {
            // 检查 (*it) 和 (*it)->txn 是否为 nullptr
            if ((*it) != nullptr && (*it)->txn != nullptr) {
                // 检查 txn 的线程 ID 是否匹配
                if ((*it)->txn->get_thd_id() == entry->txn->get_thd_id()) {
                    it = entry_list->erase(it);  // 删除元素并返回下一个有效的迭代器
                    if (waiter_cnt > 0) {
                        waiter_cnt--;  // 保证 waiter_cnt 不为负
                    }
                } else {
                    ++it;  // 如果没有删除元素，则继续迭代
                }
            } else {
                ++it;  // 如果 (*it) 或 (*it)->txn 为 nullptr，则跳过此元素
            }
        }
    }

    inline void list_rm() {
        // 直接使用 entry_list.size() 来避免依赖外部计算的 sz
        for (auto it = entry_list->begin(); it != entry_list->end(); ) {
            // 如果元素为空，直接删除它
            if (*it == nullptr) {
                it = entry_list->erase(it);  // 删除空元素，it 被更新为下一个有效迭代器
                // 减少 waiter_cnt，但保证 waiter_cnt 不小于 0
                if (waiter_cnt > 0) {
                    waiter_cnt--;
                }
            } else {
                auto status_ = (*it)->status;
                // 如果元素的状态是 LOCK_OWNER, LOCK_RETIRED, 或 LOCK_DROPPED，则删除
                if (status_ == LOCK_OWNER || status_ == LOCK_RETIRED || status_ == LOCK_DROPPED) {
                    it = entry_list->erase(it);  // 删除元素，it 被更新为下一个有效迭代器
                    // 减少 waiter_cnt，但保证 waiter_cnt 不小于 0
                    if (waiter_cnt > 0) {
                        waiter_cnt--;
                    }
                } else {
                    ++it; // 如果没有删除元素，则继续迭代
                }
            }
        }
    }

    bool bring_out_waiter(RRLockEntry * entry, txn_man * txn) {
        bool result = false;
        if (entry->txn != nullptr && !entry->txn->lock_abort){
            entry->txn->lock_ready = true;
            if (entry->type == LOCK_EX ){
                entry->status = LOCK_OWNER;
            }else {
                entry->status = LOCK_RETIRED;
            }

            if (txn == entry->txn) {
                result = true;
            }
        }

        return result;
    };
    void bring_out_waiter(RRLockEntry * entry ) {
        list_rm ( entry);

        entry->access = nullptr;
        entry->status = LOCK_DROPPED;
        entry->type = LOCK_NONE;
    };

    // push txn entry into waiter list, ordered by timestamps, ascending
    inline void add_to_waiters(ts_t ts, RRLockEntry* to_insert) {
        assert(to_insert != nullptr);
        assert(to_insert->txn != nullptr);

        bool find = false;
        RRLockEntry* en = nullptr;
        auto insert_before = entry_list->begin();

        // traverse the wait list, find the insert position
        for (auto it = entry_list->begin(); it != entry_list->end(); ++it) {
            en = *it;

            if (en == nullptr) {
                continue;
            }
            if (en->txn == nullptr) {
                continue;
            }

            if (en->access != nullptr) {
                if (en->txn != nullptr && !en->txn->lock_abort  ) {
                    if ( ts < en->txn->get_ts()) {
                        find = true;
                        insert_before = it;
                        break;
                    }
                }
            }
        }

        if (!find) {
            entry_list->push_back(to_insert);   // insert the wait tail
        } else {
            entry_list->insert(insert_before, to_insert);  // insert the find position
        }

        to_insert->status = LOCK_WAITER;
        to_insert->txn->lock_ready = false;
        waiter_cnt++;

        assert(ts != 0);
    };

    inline RRLockEntry * find_write_in_waiter(ts_t ts) {
        RRLockEntry * read = nullptr;
        RRLockEntry * en = nullptr;
        for (auto it = entry_list->begin(); it != entry_list->end(); ++it) {
            en = *it;
            if (en->type == LOCK_EX){
                if ( (en->txn != nullptr) && (en->txn->get_ts() < ts)){
                    read = en;
                    break;
                }
            }
        }

        return read;
    }

    // GC
    void remove_tombstones() {
        // remove waiters
        list_rm();

        // remove owner
        if (owner != nullptr) {
            if (owner->access == nullptr){
                owner = nullptr;
            } else {
                if (owner->txn != nullptr && owner->txn->status == ABORTED) {
                    owner = nullptr;
                }
            }
        }

        // remove retired versions
        assert(version_header != nullptr);
        while (version_header) {
            // Check conditions for skipping this version
            if (version_header->type == XP ) {
                break;
            }

            if (version_header->type == AT ) {
                // If owner is null or has no access, just remove version header
                if (owner == nullptr) {
                    version_header = version_header->next;
                    if (version_header != nullptr) {
                        version_header->prev = nullptr;  // Avoid null pointer dereferencing
                    }
                } else {
                    // Otherwise, update the owner's access and remove version header
                    if (owner->access!= nullptr){
                        if (owner->access->tuple_version->next == version_header){
                            owner->access->tuple_version->next = version_header->next;
                        }
                    }
                    version_header = version_header->next;
                    if (version_header != nullptr) {
                        version_header->prev = nullptr;  // Avoid null pointer dereferencing
                    }
                }
            } else {
                break; // Exit if conditions are not met for removal
            }
        }

        assert(version_header != nullptr);
    }

    void release_row(access_t type, RRLockEntry * entry, Version * new_version, RC rc, txn_man *curr) {
        if (rc == RCOK){
            if (entry->type == LOCK_EX && entry->status == LOCK_OWNER){
                owner = nullptr;
            } else {
                assert(entry->status == LOCK_RETIRED);
            }
        } else {
            if (type == WR ){
                if (owner != nullptr){
                    if ( entry->txn->get_thd_id() == owner->txn->get_thd_id()){
                        owner = nullptr;
                    }
                }

                if (entry->status == LOCK_WAITER){
                    bring_out_waiter( entry ) ;
                }

                new_version->type = AT;
//            new_version->retire = nullptr;
            } else {
                if (entry->status == LOCK_WAITER){
                    bring_out_waiter( entry ) ;
                }
            }
        }

        if (!owner){
            bring_next(nullptr, curr);
        }
    }

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

    bool wound_rebirth(ts_t ts, txn_man *curr_txn, TsType type) {
        //1. check the conflicts
        // find txns whose timestamp priority lower than me
        std::vector<Version *> lower_than_me;
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
                    if (owner->access != nullptr){
                        lower_than_me.push_back(owner->access->tuple_version);
                    }
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
            curr_txn->lock_abort = true;
#if PF_CS
            INC_STATS(curr_txn->get_thd_id(), blind_kill_count, 1);
            INC_STATS(curr_txn->get_thd_id(), time_rebirth,  get_sys_clock() - starttimeRB);
#endif
            return true;
        }

        int size_dep = lower_than_me.size();
        int size_sort = sortedOrder->size();
        bool find = false;
        int find_idx = 0;
        // wound the txns whose timestamp larger than me
        if (size_dep > 0){
            for (int i = size_dep - 1; i >= 0; --i){
                auto version_o = lower_than_me[i];
                auto dep_txn_o = version_o->retire;
                if (dep_txn_o != nullptr) {
                    if (find){
                        version_o->type = AT;
                        curr_txn->wound_txn(dep_txn_o);
                        dep_txn_o->lock_abort = true;
#if PF_CS
                        INC_STATS(curr_txn->get_thd_id(), find_circle_abort_depent, 1);
#endif
                    }else{
                        for (int j = 0; j < size_sort; ++j) {
                            if (sortedOrder->at(j).first == dep_txn_o->get_thd_id()){
                                version_o->type = AT;
                                curr_txn->wound_txn(dep_txn_o);
                                dep_txn_o->lock_abort = true;
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


        // rebirth the txns in my children
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
        if (size_dep > 0 && size_sort > 1){
            //option 2: just larger than all the conflict txns
            auto max_ts = ts;
            auto dep_version = lower_than_me[find_idx];
            auto dep_txn_o = dep_version->retire;
            if (dep_txn_o == nullptr){
                max_ts = std::max(max_ts, dep_version->begin_ts);
            }else{
                max_ts = std::max(max_ts, dep_txn_o->get_ts());
            }

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

            uint64_t defer_ts = curr_txn->increment_ts(max_ts);
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
                curr_txn->wound_txn(dep_txn_o);
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

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


/**
 * Version Format in REBIRTH_RETIRE
 */

#ifdef ABORT_OPTIMIZATION
// 24 bit chain_number + 40 bit deep_length
#define CHAIN_NUMBER (((1ULL << 24)-1) << 40)
#define DEEP_LENGTH ((1ULL << 40)-1)
#define CHAIN_NUMBER_ADD_ONE (1ULL << 40)
#endif

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
    volatile ts_t *dynamic_txn_ts;  //pointing to the created transaction's ts
    HReader *read_queue;
    Version* prev;
    Version* next;
    txn_man* retire;      // the txn_man of the uncommitted txn which updates the tuple version
    row_t* data;

#if ABORT_OPTIMIZATION
    uint64_t version_number;
#endif

    Version(txn_man * txn):  begin_ts(INF), end_ts(INF),retire(txn),read_queue(nullptr),type(WR) {};

    void init(){
        this->begin_ts = INF;
        this->end_ts = INF;
        this->read_queue = nullptr;
        this->retire = nullptr;
        this->type = XP;
        this->dynamic_txn_ts = new ts_t(0);
        this->prev = nullptr;
        this->version_number = 0;
    }
};


class Row_rr {
public:
    void init(row_t *row);

    RC access(txn_man *txn, TsType type, Access *access);

    Version *get_version_header() { return this->version_header; }

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
      return entry;
    }

    inline bool bring_out_waiter(LockEntry * entry, txn_man * txn) {
        LIST_RMB(waiters_head, waiters_tail, entry);
        entry->txn->lock_ready = true;
        if (txn == entry->txn) {
            return true;
        }
        return false;
    };

    // pop txn entry from waiter list, if current owner has many parents, not pop
//    void bring_next() {
//        LockEntry * entry = waiters_head;
//        LockEntry * next = NULL;
//        UInt32 traverse_sz = 0;
//
//        // If any waiter can join the owners, just do it!
//        while (entry) {
//            if (traverse_sz > waiter_cnt){
//                break;
//            }
//
//            if (entry->txn == nullptr || entry->txn->status == ABORTED || entry->txn->lock_ready) {
//                traverse_sz++;
//                LIST_RMB(waiters_head, waiters_tail, entry);
//                if (waiter_cnt > 0){
//                    waiter_cnt --;
//                }
//                entry = entry->next;
//                continue;
//            }
//
//
//            owner = entry->txn;
//            LIST_RMB(waiters_head, waiters_tail, entry);
//            if (waiter_cnt > 0){
//                waiter_cnt --;
//            }
//
//            break;
//        }
//    }
    void bring_next() {
        LockEntry *entry = waiters_head;
        LockEntry *next = NULL;
        UInt32 traverse_sz = 0;
        if (owner != nullptr) {
            if (owner->status != LOCK_OWNER) {
                owner = nullptr;
            }
        }
        // If any waiter can join the owners, just do it!
        while (entry) {
            if (traverse_sz > waiter_cnt) {
                break;
            }

            if (entry->txn == nullptr || entry->txn->status == ABORTED || entry->txn->lock_ready) {
                traverse_sz++;
                LIST_RMB(waiters_head, waiters_tail, entry);
                if (waiter_cnt > 0) {
                    waiter_cnt--;
                }
                entry = entry->next;
                continue;
            }

            if (entry->type == LOCK_EX) {
                // todo: still waitting
//                if (entry->txn->i_dependency_on.size() > 1/g_thread_cnt){
//
//                }
                if (owner == nullptr) {
                    entry->status = LOCK_OWNER;
                    entry->txn->lock_ready = true;
                    owner = entry;

                    LIST_RMB(waiters_head, waiters_tail, entry);
                    if (waiter_cnt > 0) {
                        waiter_cnt--;
                    }
                } else {
                    if (owner->status == LOCK_RETIRED || owner->status == LOCK_DROPPED) {
                        entry->status = LOCK_OWNER;
                        entry->txn->lock_ready = true;
                        owner = entry;

                        LIST_RMB(waiters_head, waiters_tail, entry);
                        if (waiter_cnt > 0) {
                            waiter_cnt--;
                        }
                    }
                }
            } else {
                entry->status = LOCK_RETIRED;
                entry->txn->lock_ready = true;

                LIST_RMB(waiters_head, waiters_tail, entry);
                if (waiter_cnt > 0) {
                    waiter_cnt--;
                }
            }

            break;
        }
    }

    // push txn entry into waiter list, ordered by timestamps, ascending
    inline void add_to_waiters(ts_t ts, LockEntry * to_insert) {
        LockEntry * en = waiters_head;
        UInt32 traverse_sz = 0;
        bool find = false;
        while (en != NULL) {
            if (traverse_sz > waiter_cnt){
                break;
            }
            if (ts < en->txn->get_ts()){
                find = true;
                break;
            }

            en = en->next;
            traverse_sz++;
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

#if LATCH == LH_SPINLOCK
    pthread_spinlock_t * spinlock_row;
#else
    mcslock * latch_row;
#endif

#if VERSION_CHAIN_CONTROL
    // Restrict the length of version chain.
    uint64_t threshold;

    void IncreaseThreshold(){
        ATOM_ADD(threshold,1);
    }

    void DecreaseThreshold(){
        ATOM_SUB(threshold,1);
    }
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

    // check priorities
    inline static bool a_higher_than_b(ts_t a, ts_t b) {
        return a < b;
    };

    inline static int assign_ts(ts_t ts, txn_man *txn) {
        if (ts == 0) {
            ts = txn->set_next_ts(1);
            // if fail to assign, reload
            if (ts == 0) {
                ts = txn->get_ts();
            }
        }
        return ts;
    };

    // 递归函数来添加依赖关系
    void addDependencies(std::unordered_map<uint64_t, std::vector<txn_man*>*> *adjacencyList,
                         txn_man *txn) {
        auto direct_depents = txn->rr_dependency;
        auto *curr_depts = new std::vector<txn_man *>();
        adjacencyList->insert(std::make_pair(txn->get_thd_id(), curr_depts));

        std::stack<txn_man *> dep_stack;
        for (auto &dep_pair: *direct_depents) {
            auto dep_txn = dep_pair.dep_txn;
            if (dep_txn != nullptr && dep_txn->status == RUNNING ) {
                dep_stack.push(dep_txn);
                adjacencyList->at(txn->get_thd_id())->push_back(dep_txn); // to <- from
            }
        }

        while (true){
            std::vector<txn_man *> dep_list;
            while (!dep_stack.empty()){
                txn_man *txn_ = dep_stack.top();
                if (txn_ != nullptr && txn_->status == RUNNING){
                    dep_list.push_back(txn_);
                }
                dep_stack.pop();
            }

            for (auto &dep_pair: dep_list) {
                auto dep_txn_ = dep_pair;
                if (dep_txn_ != nullptr && dep_txn_->status == RUNNING) {
                    if (adjacencyList->find(dep_txn_->get_thd_id()) != adjacencyList->end()) continue;

                    auto *curr_depts = new std::vector<txn_man *>();
                    adjacencyList->insert(std::make_pair(dep_txn_->get_thd_id(), curr_depts));

                    auto dep_txn_deps = dep_txn_->rr_dependency;
                    if (!dep_txn_deps->empty()){
                        for (auto &dep_: *dep_txn_deps) {
                            if (dep_.dep_txn != nullptr && dep_.dep_txn->status == RUNNING){
                                dep_stack.push(dep_.dep_txn);
                                adjacencyList->at(dep_txn_->get_thd_id())->push_back(dep_.dep_txn);
                            }
                        }
                    }
                }
            }

            if (dep_stack.empty()){
                break;
            }
        }
    }

    bool buildGraph(std::unordered_map<uint64_t, std::vector<txn_man*> *> *adjacencyList,
                    txn_man *txn) {
        if (txn == nullptr || txn->status == ABORTED) return false;
        // 开始递归构建依赖图
        addDependencies(adjacencyList, txn);
        if (adjacencyList->empty()) return false;

        return true;
    }

    bool topologicalSort(std::unordered_map<uint64_t, std::vector<txn_man*> *> *adjacencyList,
                         std::vector<uint64_t> *sortedOrder) {
        std::unordered_map<uint64_t, int> inDegree;

        // 计算每个节点的入度
        for (const auto& pair : *adjacencyList) {
            uint64_t thd_id = pair.first;
            inDegree[thd_id] = 0; // 初始化入度

            for (const auto& dep_txn : *(pair.second)) {
                if (dep_txn != nullptr && dep_txn->status == RUNNING) {
                    inDegree[dep_txn->get_thd_id()]++; // dep_txn 依赖于 thd_id
                }
            }
        }

        // 初始化队列，将入度为0的节点加入
        std::queue<uint64_t> zeroInDegreeQueue;
        for (const auto& pair : inDegree) {
            if (pair.second == 0) {
                zeroInDegreeQueue.push(pair.first);
            }
        }

        // Kahn's 算法进行拓扑排序
        while (!zeroInDegreeQueue.empty()) {
            uint64_t thd_id = zeroInDegreeQueue.front();
            zeroInDegreeQueue.pop();

#if !KEY_ORDER
            auto itr = std::find(sortedOrder->begin(), sortedOrder->end(), thd_id);
            if (itr == sortedOrder->end()){
                sortedOrder->push_back(thd_id);
            }
#endif

            // 遍历该节点的邻居，减少入度
            auto it = adjacencyList->find(thd_id);
            if (it != adjacencyList->end()) {
#if KEY_ORDER
                if (sortedOrder->find(thd_id) == sortedOrder->end()){
                    sortedOrder->insert(thd_id);
                }
#endif
                for (const auto& dep_txn : *(it->second)) {
                    uint64_t dep_id = dep_txn->get_thd_id(); // 依赖于 thd_id 的事务

                    // 减少入度
                    if (inDegree[dep_id] > 0) {
                        inDegree[dep_id]--;

                        // 如果入度减为0，加入队列
                        if (inDegree[dep_id] == 0) {
                            zeroInDegreeQueue.push(dep_id);
                        }
                    }
                }
            }
        }

        // 检查是否有环
        if (sortedOrder->size() != adjacencyList->size()) {
            return true;
        }

        return false; // 无环
    }

    bool wound_rebirth(ts_t ts, txn_man *curr_txn, TsType type){
        //1. check the conflicts
        // find txns whose timestamp priority lower than me
        auto lower_than_me = std::vector<txn_man *>();
        auto wound_header = version_header;
        uint32_t tuple_retire_num = 0;
        while (wound_header){
            if (wound_header->retire == nullptr) break;
            if (wound_header->type == AT) {
                wound_header = wound_header->next;
                continue;
            }

            auto write_txn_ = wound_header->retire;
            tuple_retire_num ++;
            //if find a higher write txn
            if (write_txn_ != nullptr && write_txn_->status == RUNNING){
                if (a_higher_than_b(curr_txn->get_ts(), write_txn_->get_ts())){
                    lower_than_me.push_back(write_txn_);
                }
            }

            wound_header = wound_header->next;
        }

        if (lower_than_me.size() <= 0) return false;

        bool rebirth = true;
//        uint32_t children_num = curr_txn->hotspot_friendly_dependency->size();
//        uint32_t tuple_owner_num = 0;
//        if (owner) tuple_owner_num++;
//        double threold = (double)(children_num + tuple_retire_num + tuple_owner_num) / (g_thread_cnt-1);
////        printf("threold:%f, \n", threold);
//        if (threold > 0.05){
//            rebirth = false;
//        }

#if REBIRTH
//if (rebirth){
        uint64_t starttimeRB = get_sys_clock();
        //2. use Kahn's algorithm for topologicalSort
        if (curr_txn->status == ABORTED || curr_txn->rr_dependency->empty()) return false;
        // build dependent graph
        auto *adjacencyList = new std::unordered_map<uint64_t, std::vector<txn_man*> *>(); // to<-from
        auto ret = buildGraph(adjacencyList, curr_txn);
        if (!ret) return false;
        auto *sortedOrder = new std::vector<uint64_t>();
        auto ret1 = topologicalSort(adjacencyList, sortedOrder);
        if (ret1){
//#if PF_CS
//            uint64_t time_wound = get_sys_clock();
//#endif
            curr_txn->set_abort();
#if PF_CS
//            INC_STATS(curr_txn->get_thd_id(), time_wound, get_sys_clock() - time_wound);
            INC_STATS(curr_txn->get_thd_id(), find_circle_abort_depent, 1);
            INC_STATS(curr_txn->get_thd_id(), time_rebirth,  get_sys_clock() - starttimeRB);
#endif
//#if PF_ABORT
//            curr_txn->wound = true;
//#endif
            return true;
        }

        //3. do rebirth, detect cycle on the retires and owners
        auto size_dep = lower_than_me.size();
        auto size_sort = sortedOrder->size();
        bool find = false;
        int find_idx = 0;
#if KEY_ORDER == false
        if (size_dep > 0){
            for (int i = size_dep - 1; i >= 0; --i){
                auto dep_txn_o=lower_than_me[i];
                if (dep_txn_o != nullptr) {
                    if (find){
                        curr_txn->wound_txn(dep_txn_o);
//#if PF_CS
//                        INC_STATS(curr_txn->get_thd_id(), find_circle_abort_depent, 1);
//#endif
//#if PF_ABORT
//                        dep_txn_o->wound = true;
//#endif
                    }else{
                        for (int j = 0; j < size_sort; ++j) {
                            if (sortedOrder->at(j) == dep_txn_o->get_thd_id()){
                                curr_txn->wound_txn(dep_txn_o);
                                find = true;
                                find_idx = i;
//#if PF_CS
//                                INC_STATS(curr_txn->get_thd_id(), find_circle_abort_depent, 1);
//#endif
//#if PF_ABORT
//                        dep_txn_o->wound = true;
//#endif
                            }
                        }
                    }
                }
            }

        }
#endif

        // 3. do rebirth, update my children' ts
#if NEXT_TS
        if (size_dep > 0){
            //option 1: the next TS assigned
            uint64_t next_ts = glob_manager->get_ts(curr_txn->get_thd_id());
            curr_txn->set_ts(next_ts);
            for (auto it = sortedOrder->begin(); it != sortedOrder->end(); ++it) {
                auto depent_txn_thd_id = *it;
                auto depent_txn = glob_manager->get_txn_man(depent_txn_thd_id);
                next_ts = glob_manager->get_ts(depent_txn_thd_id);
                depent_txn->set_ts(next_ts);
            }
        }
#else
        if (size_dep > 0){
            //option 2: just larger than all the conflict txns
            auto max_ts = ts;
            auto dep_txn_o = lower_than_me[find_idx];
            max_ts = std::max(max_ts, dep_txn_o->get_ts());
            auto readers = wound_header->read_queue;
            HReader *read_ = nullptr;
            if (readers != nullptr) {
                read_ = readers;
                while (true) {
                    if (read_->next == nullptr) break;
                    auto read_txn_ = read_->cur_reader;
                    if (read_txn_ != nullptr && read_txn_->status == RUNNING) {
                        max_ts = std::max(max_ts, read_txn_->get_ts());
                    }

                    read_ = read_->next;
                }
            }

            uint64_t defer_ts = max_ts + 1;
            curr_txn->set_ts(defer_ts);
            for (auto it = sortedOrder->begin(); it != sortedOrder->end(); ++it) {
                auto depent_txn_thd_id = *it;
                auto depent_txn = glob_manager->get_txn_man(depent_txn_thd_id);
                if (depent_txn->status == RUNNING && depent_txn->get_ts() < defer_ts){
                    defer_ts++;
                    depent_txn->set_ts(defer_ts);
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
//     } else {
        auto size_dep = lower_than_me.size();
        for (int i = 0; i > size_dep; ++i) {
            auto dep_txn_o = lower_than_me[i];
            if (dep_txn_o != nullptr) {
                dep_txn_o->set_abort();
            }
        }

        return false;
//     }

#endif

    }


private:

//    Version * _write_history; // circular buffer, convert it to the thread class

};

#endif


#endif

//
// Created by root on 2024/7/22.
//
#include "txn.h"
#include "row.h"
#include "row_rr.h"
#include "manager.h"
#include <mm_malloc.h>
#include <unordered_set>
#include "thread.h"


#if CC_ALG == REBIRTH_RETIRE

#define decrease_semaphore( semaphore) { \
	if (semaphore >0) semaphore--; }      \


RC txn_man::retire_row(int access_cnt){
    if (this->status == ABORTED || this->lock_abort){
        return Abort;
    } else {
        return accesses[access_cnt]->orig_row->retire_row(accesses[access_cnt]->lock_entry);
    }
}

bool cycle_recheck(std::unordered_set<uint64_t> *i_depents, txn_man *txn){
    auto *adjacencyList = new std::unordered_map<uint64_t, std::set<txn_man*> *>(); // to<-from
    auto ret = txn->buildGraph(adjacencyList, txn);
    bool cycle = false;
    std::vector<std::pair<uint64_t, std::pair<uint64_t , uint64_t>>> *sortedOrder;
    if (ret){
        sortedOrder = new std::vector<std::pair<uint64_t, std::pair<uint64_t , uint64_t>>>();
        cycle = txn->topologicalSort(adjacencyList, sortedOrder, i_depents);
    }
    if (cycle){
        return true;
    }

    return false;
}

RC txn_man::validate_rr(RC rc) {
    if(rc == Abort || status == ABORTED){

        abort_process(this);
        return Abort;
    }

    uint64_t serial_id = 0;
    serial_id =  this->get_ts() ;
    bool ts_zero = false;
    if(serial_id == 0){
        // traverse the reads and writes, max of read version and max+1 of write version
        for(int rid = 0; rid < row_cnt; rid++) {
            auto new_version = accesses[rid]->tuple_version;
            auto old_version = accesses[rid]->old_version;
            if (accesses[rid]->type == RD) {
                if (new_version == nullptr) continue;
                if (new_version->type == AT ) continue;
                serial_id = std::max(new_version->begin_ts, serial_id);
            }
            if (accesses[rid]->type == WR){
                if (old_version->type == AT) continue;
                uint64_t ts = 0;
                auto retire_old = old_version->retire;
                if (retire_old == nullptr){
                    ts = increment_ts(old_version->begin_ts);
                } else {
                    ts = increment_ts(retire_old->get_ts());
                }
                serial_id = std::max(ts, serial_id);
            }
        }

        serial_id =  increment_ts(serial_id);
        ts_zero = true;
    }
    assert(serial_id != INF);
//    this->set_ts(serial_id);

    uint32_t i_dependency_on_size = parents.size();
    uint32_t i_dependency_semaphore = i_dependency_on_size;

    auto *i_depents = new std::unordered_set<uint64_t>();
    bool has_check = false;

    uint64_t starttime = get_sys_clock();
    while(true) {
        if(status == ABORTED){
            rc = Abort;
            break;
        }

        if (i_dependency_semaphore == 0) {
            break;
        }
        else {
            for (auto & it : parents) {
                auto depend_txn = it.first;
                if (depend_txn == nullptr || depend_txn->status == COMMITED) {
                    decrease_semaphore(i_dependency_semaphore);
                }

                if (depend_txn->status == ABORTED){
                    if (it.second != READ_WRITE_){
                        this->status = ABORTED;
                        break;
                    } else{
                        decrease_semaphore(i_dependency_semaphore);
                    }
                }

                if (depend_txn->status == validating ){
                    auto depend_txn_ts = depend_txn->get_ts();
                    if (depend_txn_ts < serial_id){
                        decrease_semaphore(i_dependency_semaphore);
                    }else {
                        if (!has_check){
                            has_check = true;
                            auto has_cycle = cycle_recheck(i_depents, this);
                            if (has_cycle){
                                this->status = ABORTED;
                                break;
                            }
                        }

                        auto itr = i_depents->find(depend_txn->get_txn_id());
                        if ( itr != i_depents->end()) {
                            this->status = ABORTED;
                            break;
                        }else{
                            serial_id = increment_ts(depend_txn_ts) ;
                            decrease_semaphore(i_dependency_semaphore);
                        }
                    }
                }
            }
        }
    }

#if PF_CS
    uint64_t endtime_commit = get_sys_clock();
    uint64_t timespan1 = endtime_commit - starttime;
    INC_STATS(this->get_thd_id(), time_commit, timespan1);
    this->wait_latch_time = this->wait_latch_time + timespan1;
#endif

    if (rc == Abort || status == ABORTED){
        abort_process(this);
        return Abort;
    }

#if PF_CS
    uint64_t startt_latch = get_sys_clock();
#endif
    for(int rid = 0; rid < row_cnt; rid++){
        accesses[rid]->lock_entry->status = LOCK_RETIRED;
        if (accesses[rid]->type == RD){
            continue;
        }

        accesses[rid]->orig_row->manager->lock_row(this);
        auto new_version = accesses[rid]->tuple_version;
        assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);
        auto old_version = accesses[rid]->old_version;
        if (old_version->type == AT){
            while (true){
                old_version = old_version->next;
                if (old_version != nullptr &&
                    (old_version->type == XP || (old_version->type == WR && old_version->retire->status != ABORTED))){
                    break;
                }
            }
        }
        // this is because, when i read the old version, it is validating, has no dependency
        if (serial_id <= old_version->begin_ts){
            if (old_version->type == XP){
                serial_id = increment_ts(old_version->begin_ts);
            } else if (old_version->type == WR){
                auto retire_txn = old_version->retire;
                if (retire_txn != nullptr){
                    serial_id = increment_ts(retire_txn->get_ts());
                }
            }
        }
        old_version->end_ts = serial_id;
        new_version->begin_ts = serial_id;
        new_version->retire = nullptr;
        new_version->type = XP;

        auto en = accesses[rid]->lock_entry;
        auto type = accesses[rid]->type;
        accesses[rid]->orig_row->manager->release_row(type, en, nullptr, RCOK, this);

        accesses[rid]->orig_row->manager->unlock_row(this);
    }

#if PF_CS
    uint64_t timespan = get_sys_clock() - startt_latch;
    INC_STATS(this->get_thd_id(), time_get_cs, timespan);
    this->wait_latch_time = this->wait_latch_time + timespan;
#endif

    ATOM_CAS(status, validating, COMMITED);

    parents.clear();
    children.clear();

    return rc;
}

void txn_man::abort_process(txn_man * txn ){
    auto status_ = txn->status;
    ATOM_CAS(status, status_, ABORTED);
    txn->lock_abort = true;
    COMPILER_BARRIER

    for(int rid = 0; rid < row_cnt; rid++) {
//        if (accesses[rid]->type == RD) {
//            continue;
//        }

//        assert(accesses[rid]->type == WR);
        accesses[rid]->orig_row->manager->lock_row(txn);
        auto en = accesses[rid]->lock_entry;
        auto version = accesses[rid]->tuple_version;
        auto type = accesses[rid]->type;
        accesses[rid]->orig_row->manager->release_row(type, en, version, Abort, this);
        accesses[rid]->orig_row->manager->unlock_row(txn);
    }

    for(auto & dep_pair : children){
        // only inform the txn which wasn't aborted
        if (dep_pair.second != READ_WRITE_){
            if (dep_pair.first != nullptr){
                if (dep_pair.first->status == RUNNING  ) {
                    dep_pair.first->set_abort(5);
                    dep_pair.first->lock_abort = true;
#if PF_CS
                    INC_STATS(this->get_thd_id(), cascading_abort_cnt, 1);
#endif
                }
            }
        }
    }

    parents.clear();
    children.clear();
}


void txn_man::addDependencies(std::unordered_map<uint64_t, std::set<txn_man*>*> *adjacencyList,
                              txn_man *txn) {
    auto direct_depents = (txn->children);
    auto *curr_depts = new std::set<txn_man *>();
    adjacencyList->insert(std::make_pair(txn->get_thd_id(), curr_depts));

    std::stack<txn_man *> dep_stack;
    for (auto &dep_pair: direct_depents) {
        auto dep_txn = dep_pair.first;
        if (dep_txn != nullptr && dep_txn->status == RUNNING ) {
            dep_stack.push(dep_txn);
            adjacencyList->at(txn->get_thd_id())->insert(dep_txn); // to <- from
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

                auto *curr_depts = new std::set<txn_man *>();
                adjacencyList->insert(std::make_pair(dep_txn_->get_thd_id(), curr_depts));

                auto dep_txn_deps = (dep_txn_->children);
                if (!dep_txn_deps.empty()){
                    for (auto &dep_: dep_txn_deps) {
                        if (dep_.first != nullptr && dep_.first->status == RUNNING){
                            dep_stack.push(dep_.first);
                            adjacencyList->at(dep_txn_->get_thd_id())->insert(dep_.first);
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

bool txn_man::buildGraph(std::unordered_map<uint64_t, std::set<txn_man*> *> *adjacencyList,
                         txn_man *txn) {
    if (txn == nullptr || txn->status == ABORTED) return false;
    addDependencies(adjacencyList, txn);
    if (adjacencyList->empty()) return false;

    return true;
}

bool txn_man::topologicalSort(std::unordered_map<uint64_t, std::set<txn_man*> *> *adjacencyList,
                              std::vector<std::pair<uint64_t, std::pair<uint64_t , uint64_t>>> *sortedOrder,
                              std::unordered_set<uint64_t> * i_depents) {
    std::unordered_map<uint64_t, int> inDegree;
    if (adjacencyList->empty()) {
        return false; // No cycle in an empty graph
    }
    std::set<uint64_t> nodes;
    for (const auto& pair : *adjacencyList) {
        nodes.insert(pair.first);
        for (const auto& dep_txn : *(pair.second)) {
            if (dep_txn != nullptr) {
                nodes.insert(dep_txn->get_thd_id());
            }
        }
    }

    // 计算每个节点的入度
    for (const auto& pair : *adjacencyList) {
        uint64_t thd_id = pair.first;
        inDegree[thd_id] = 0; // 初始化入度

        for (const auto& dep_txn : *(pair.second)) {
            if (dep_txn != nullptr ) {
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

        bool find = false;
        for (int i = 0; i < sortedOrder->size(); ++i) {
            if (sortedOrder->at(i).first == thd_id) {
                find = true;
                break;
            }
        }
        if (!find){
            auto txn = glob_manager->get_txn_man(thd_id);
            auto timestmp_v = txn->timestamp_v.load();
            auto mk = std::make_pair(txn->get_ts(), timestmp_v);
            auto mk_ = std::make_pair(thd_id, mk);
            sortedOrder->push_back(mk_);
            if (i_depents != nullptr){
                i_depents->insert(txn->get_txn_id());
            }
        }

        // 遍历该节点的邻居，减少入度
        auto it = adjacencyList->find(thd_id);
        if (it != adjacencyList->end()) {
            for (const auto& dep_txn : *(it->second)) {
                if (dep_txn != nullptr) {
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
    }

    if (inDegree.empty() || zeroInDegreeQueue.empty()) return false;

    // 检查是否有环
    if (sortedOrder->size() != nodes.size()) {
        return true;
    }

    // 排序sortedOrder，按照时间戳从小到大排序
    std::sort(sortedOrder->begin(), sortedOrder->end(),
              [](const std::pair<uint64_t, std::pair<uint64_t , uint64_t>>& a,
                 const std::pair<uint64_t, std::pair<uint64_t , uint64_t>>& b) {
                  return a.second.first < b.second.first;
              });

    return false; // 无环
}

#endif


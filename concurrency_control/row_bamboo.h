#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

//#if CC_ALG == BAMBOO

// update cohead info when a newly-init entry (en) is firstly
// added to owners (bring_next, WR)
// or tail of retired (lock_get/bring_next, RD)
// (not apply when moving from owners to retired
// Algorithm:
//     if previous entry is not null
//         if self is WR,
//             not cohead, need to incr barrier
//         else
//             if previous entry is RD
//               if prev is cohead,
//                   //time saved is 0.
//               otherwise, self is not cohead, need to incr barrier,
//                   //record start_ts to calc time saved when becomes cohead
//             else if prev is WR
//               not cohead, need to incr barrier,
//               //record start_ts to calc time saved when becomes cohead
//     else
//         read no dirty data, becomes cohead
//         //record time saved from elr is 0.
//
#include <stack>

#define UPDATE_RETIRE_INFO(en, prev) { \
  if (prev) { \
    if (en->type == LOCK_EX) \
      en->txn->increment_commit_barriers(); \
    else { \
        if (prev->type == LOCK_SH) { \
          en->is_cohead = prev->is_cohead; \
          if (!en->is_cohead) { \
            en->txn->increment_commit_barriers(); \
          } \
        } else { \
          en->txn->increment_commit_barriers(); } \
    } \
  } else { \
    en->is_cohead = true; \
   } }

// used by lock_retire() (move from owners to retired)
// or by lock_get()/bring_next(), used when has no owners but directly enters retired
// for the latter need to call UPDATE_RETIRE_INFO(to_insert, retired_tail);
#define ADD_TO_RETIRED_TAIL(to_retire) { \
  LIST_PUT_TAIL(retired_head, retired_tail, to_retire); \
  to_retire->status = LOCK_RETIRED; \
  retired_cnt++; }

// Insert to_insert(RD) into the tail when owners is not empty
// (1) update inserted entry's cohead information
// (2) NEED to update owners cohead information
//     if owner is not cohead, it cannot become one with RD inserted
//     if owner is cohead, RD becomes cohead and owner is no longer a cohead
#define INSERT_TO_RETIRED_TAIL(to_insert) { \
  UPDATE_RETIRE_INFO(to_insert, retired_tail); \
  if (owners && owners->is_cohead) { \
    owners->is_cohead = false; \
    owners->txn->increment_commit_barriers(); \
  } \
  LIST_PUT_TAIL(retired_head, retired_tail, to_insert); \
  to_insert->status = LOCK_RETIRED; \
  retired_cnt++; }

#define RETIRE_ENTRY(to_retire) { \
  to_retire = owners; \
  owners = NULL; \
  to_retire->next=NULL; \
  to_retire->prev=NULL; \
  ADD_TO_RETIRED_TAIL(to_retire); }

#define CHECK_ROLL_BACK(en) { \
    en->access->orig_row->copy(en->access->orig_data); \
}

#define DEC_BARRIER_PF(entry) { \
    assert(!entry->is_cohead); \
    entry->is_cohead = true; \
    uint64_t starttime = get_sys_clock(); \
    entry->txn->decrement_commit_barriers(); \
    INC_STATS(entry->txn->get_thd_id(), time_semaphore_cs, \
        get_sys_clock() - starttime); \
}

#define DEC_BARRIER(entry) { \
    assert(!entry->is_cohead); \
    entry->is_cohead = true; \
    entry->txn->decrement_commit_barriers(); \
}

struct BBLockEntry {
    // type of lock: EX or SH
    txn_man * txn;
    Access * access;
    //uint8_t padding[64 - sizeof(void *)*2];
    lock_t type;
    uint8_t padding[64 - sizeof(void *)*2 - sizeof(lock_t)];
    BBLockEntry * next;
    //uint8_t padding[64 - sizeof(void *)*3 - sizeof(lock_t)];
    bool is_cohead;
    lock_status status;
    BBLockEntry * prev;
    BBLockEntry(txn_man * t, Access * a): txn(t), access(a), type(LOCK_NONE),
                                          next(NULL), is_cohead(false),
                                          status(LOCK_DROPPED),
                                          prev(NULL) {};
};

using DirectedGraph = std::unordered_map<uint64_t, std::vector<uint64_t> *>;
enum class NodeStatus {
    NotVisited,
    Visiting,
    Visited
};

inline uint64_t DFS(DirectedGraph& graph, std::unordered_map<int, NodeStatus>& status,
                    uint64_t startNode, std::unordered_set<uint64_t>& currentPath,
                    uint64_t& lastNode ) {
    std::stack<uint64_t> nodeStack;
    nodeStack.push(startNode);
    status[startNode] = NodeStatus::Visiting;

    while (!nodeStack.empty()) {
        uint64_t node = nodeStack.top();

        if (graph.find(node) == graph.end()) {
            nodeStack.pop();
            currentPath.erase(node);
            status[node] = NodeStatus::Visited;
            continue;
        }

        currentPath.insert(node);
        lastNode = node;

        bool hasUnvisitedNeighbor = false;
        for (const auto& neighbor : *(graph.at(node))) {
            if (currentPath.find(neighbor) != currentPath.end()) {
                // 发现环，将环上的节点标记为已访问状态
                for (const auto& nodeInPath : currentPath) {
                    status[nodeInPath] = NodeStatus::Visited;
                }
                nodeStack.pop();
//                if (!nodeStack.empty()){
//                    lastNode = nodeStack.top();
//                }

                return lastNode;
            } else if (status[neighbor] == NodeStatus::NotVisited) {
                status[neighbor] = NodeStatus::Visiting;
                nodeStack.push(neighbor);
                hasUnvisitedNeighbor = true;
                break;
            }
        }

        if (!hasUnvisitedNeighbor) {
            nodeStack.pop();
            currentPath.erase(node);
            status[node] = NodeStatus::Visited;
        }
    }

    return 0;
}

#include <random>
#include <iostream>

class Row_bamboo {
public:
    void init(row_t * row);
    RC lock_get(lock_t type, txn_man * txn, Access * access);
    RC lock_release(BBLockEntry * entry, RC rc);
    RC lock_retire(BBLockEntry * entry);

    const double SAMPLE_PROBABILITY = 0.3;  // 即1%

private:
    // data structure
    BBLockEntry * owners;
    BBLockEntry * retired_head;
    BBLockEntry * retired_tail;
    BBLockEntry * waiters_head;
    BBLockEntry * waiters_tail;
    row_t * _row;
    UInt32 waiter_cnt;
    UInt32 retired_cnt;
    // latches
#if LATCH == LH_SPINLOCK
    pthread_spinlock_t * latch;
#elif LATCH == LH_MUTEX
    pthread_mutex_t * latch;
#else
    mcslock * latch;
#endif
    bool blatch;

    // helper functions
    bool              bring_next(txn_man * txn);
    void              update_entry(BBLockEntry * en);
    BBLockEntry *     rm_from_retired(BBLockEntry * en, bool is_abort, txn_man * txn);
    BBLockEntry *     remove_descendants(BBLockEntry * en, txn_man * txn);
    void              lock(txn_man * txn);
    void              unlock(txn_man * txn);
    RC                insert_read_to_retired(BBLockEntry * to_insert, ts_t ts, Access * access);
#if DEBUG_BAMBOO
    void              check_correctness();
#endif


    // check priorities
    inline static bool a_higher_than_b(ts_t a, ts_t b) {
        return a < b;
    };

    inline static int assign_ts(ts_t ts, txn_man * txn) {
        if (ts == 0) {
            ts = txn->set_next_ts();
            // if fail to assign, reload
            if ( ts == 0 )
                ts = txn->get_ts();
        }
        return ts;
    };

    // init a lock entry (pre-allocated in each txn's access)
    static BBLockEntry * get_entry(Access * access) {
#if CC_ALG == BAMBOO
        BBLockEntry * entry = access->lock_entry;
        entry->txn->lock_ready = false;
        // dont init lock_abort, can only be set true but not false.
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
        entry->is_cohead = false;
        return entry;
#else
        return NULL;
#endif
    };


    // clean the lock entry
    void return_entry(BBLockEntry * entry) {
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
    };

    inline bool bring_out_waiter(BBLockEntry * entry, txn_man * txn) {
        LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
        entry->txn->lock_ready = true;
        if (txn == entry->txn) {
            return true;
        }
        return false;
    };

    inline void add_to_waiters(ts_t ts, BBLockEntry * to_insert) {
        BBLockEntry * en = waiters_head;
        while (en != NULL) {
            if (ts < en->txn->get_ts())
                break;
            en = en->next;
        }
        if (en) {
            LIST_INSERT_BEFORE(en, to_insert);
            if (en == waiters_head)
                waiters_head = to_insert;
        } else {
            LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert);
        }
        to_insert->status = LOCK_WAITER;
        to_insert->txn->lock_ready = false;
        waiter_cnt++;
        assert(ts != 0);
    };

    // NOTE: it is unrealistic to have completely ordered read with
    // dynamically assigned ts. e.g. [0,0,0] -> [12, 11, 5]
    // used when to_insert->type = LOCK_SH
    inline RC wound_retired_rd(ts_t ts, BBLockEntry * to_insert) {
        BBLockEntry * en = retired_head;
        while(en) {
            if (en->type == LOCK_EX && a_higher_than_b(ts, en->txn->get_ts())) {
#if TEST_BB_ABORT
                if (en->txn->WaitingSetContains(to_insert->txn->get_txn_id()) && en->txn->status == RUNNING) {
                    // bb wound dependency, if dependency's waiting contains to_insert
                } else{
                    // if not, not exist cycle, but wound
                    INC_STATS( to_insert->txn->get_thd_id(), blind_kill_count, 1);
                }
#endif
#if ADAPTIVE
                auto ret = to_insert->txn->insert_hotspot(this->_row->get_primary_key());
                if (ret){
                    INC_STATS( to_insert->txn->get_thd_id(), abort_hotspot, 1);
                }
                INC_STATS( to_insert->txn->get_thd_id(), abort_position, to_insert->txn->row_cnt);
                INC_STATS( to_insert->txn->get_thd_id(), abort_position_cnt, 1);
#endif
                if (to_insert->txn->wound_txn(en->txn) == COMMITED) {
                    //return_entry(to_insert);
                    //return Abort;
                    en = en->next;
                    continue;
                }
                en = rm_from_retired(en, true, to_insert->txn);
            } else
                en = en->next;
        }
        return RCOK;
    };

    // try_wound(to_wound, wounder), if commited, wound failed, return wounder
    inline RC wound_retired_wr(ts_t ts, BBLockEntry * to_insert) {
        BBLockEntry * en = retired_head;
//        int ownsz = 0;
//        if (owners) ownsz = 1;
        while(en) {
            if (en->txn->get_ts() == 0 || a_higher_than_b(ts, en->txn->get_ts())) {
#if TEST_BB_ABORT
                if (en->txn->WaitingSetContains(to_insert->txn->get_txn_id()) && en->txn->status == RUNNING) {
                    // bb wound dependency, if dependency's waiting contains to_insert
                } else{
                    // if not, not exist cycle, but wound
                    INC_STATS( to_insert->txn->get_thd_id(), blind_kill_count, 1);
                }
#endif
#if ADAPTIVE
                auto ret = to_insert->txn->insert_hotspot(this->_row->get_primary_key());
                if (ret){
                    INC_STATS( to_insert->txn->get_thd_id(), abort_hotspot, 1);
                }
                INC_STATS( to_insert->txn->get_thd_id(), abort_position, to_insert->txn->row_cnt);
                INC_STATS( to_insert->txn->get_thd_id(), abort_position_cnt, 1);
#endif
                if (to_insert->txn->wound_txn(en->txn) == COMMITED) {
                    //return_entry(to_insert);
                    //return Abort;
                    en = en->next;
                    continue;
                }

#if PF_ABORT && WOUND_TEST
                auto timespan = get_sys_clock() - en->txn->start_sys_clock;
                en->txn->wound = true;
                uint32_t i_depend =0;
                uint32_t depent =0;
#if BB_TRACK_DEPENDENS
                i_depend = to_insert->txn->i_depend_set->size();
                depent = to_insert->txn->dependend_on_me->size();
#endif
                auto access_cnt = to_insert->txn->row_cnt;
                std::string ss;
                ss = "wound_entry = curr_start_timespan: " + std::to_string(timespan) + ", thdid: " + std::to_string(to_insert->txn->get_thd_id()) +
                     ", wound_txn_id: " + std::to_string(to_insert->txn->get_txn_id()) + ", has accessed: " + std::to_string(access_cnt) + ", depends_sz: " + std::to_string(i_depend) +
                     ", depents_sz: " + std::to_string(depent) + ", retire_sz: " + std::to_string(this->retired_cnt) +
                     ", wait_sz: " + std::to_string(this->waiter_cnt) + ", own_sz: " +std::to_string(ownsz);
                to_insert->txn->insert_wound(ss);
#endif

                en = rm_from_retired(en, true, to_insert->txn);
            } else
                en = en->next;
        }
        return RCOK;
    };

    inline RC wound_owner(BBLockEntry * to_insert) {
#if TEST_BB_ABORT
        if (owners->txn->WaitingSetContains(to_insert->txn->get_txn_id()) && owners->txn->status == RUNNING) {
            // bb wound owner, if owner's waiting contains to_insert
        } else{
            // if not, not exist cycle, but wound
            INC_STATS( to_insert->txn->get_thd_id(), blind_kill_count, 1);
        }
#endif
#if ADAPTIVE
        auto ret = to_insert->txn->insert_hotspot(this->_row->get_primary_key());
        if (ret){
            INC_STATS( to_insert->txn->get_thd_id(), abort_hotspot, 1);
        }
        INC_STATS( to_insert->txn->get_thd_id(), abort_position, to_insert->txn->row_cnt);
        INC_STATS( to_insert->txn->get_thd_id(), abort_position_cnt, 1);
#endif
        if (to_insert->txn->wound_txn(owners->txn) == COMMITED) {
            //return_entry(to_insert);
            //return Abort;
            return WAIT;
        }

#if PF_ABORT && WOUND_TEST
        auto timespan = get_sys_clock() - owners->txn->start_sys_clock;
        owners->txn->wound = true;
        uint32_t i_depend =0;
        uint32_t depent =0;
#if BB_TRACK_DEPENDENS
        i_depend = to_insert->txn->i_depend_set->size();
        depent = to_insert->txn->dependend_on_me->size();
#endif
        auto access_cnt = to_insert->txn->row_cnt;
        std::string ss;
        ss = "wound_entry = curr_start_timespan: " + std::to_string(timespan) + ", thdid: " + std::to_string(to_insert->txn->get_thd_id()) +
             ", wound_txn_id: " + std::to_string(to_insert->txn->get_txn_id()) + ", has accessed: " + std::to_string(access_cnt) + ", depends_sz: " + std::to_string(i_depend) +
             ", depents_sz: " + std::to_string(depent) + ", retire_sz: " + std::to_string(this->retired_cnt) +
             ", wait_sz: " + std::to_string(this->waiter_cnt) + ", own_sz: " +std::to_string(1);
        to_insert->txn->insert_wound(ss);
#endif

        return_entry(owners);
        owners = NULL;
        return RCOK;
    };

// owner's type is always LOCK_EX
#define WOUND_OWNER(to_insert) { \
    TRY_WOUND(owners, to_insert); \
    return_entry(owners); \
    owners = NULL; \
}


};
#endif

//#endif
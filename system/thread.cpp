#include <sched.h>
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "test.h"

void thread_t::init(uint64_t thd_id, workload * workload) {
    _thd_id = thd_id;
    _wl = workload;
    srand48_r((_thd_id + 1) * get_sys_clock(), &buffer);
    _abort_buffer_size = ABORT_BUFFER_SIZE;
    _abort_buffer = (AbortBufferEntry *) _mm_malloc(sizeof(AbortBufferEntry) * _abort_buffer_size, 64);
    for (int i = 0; i < _abort_buffer_size; i++)
        _abort_buffer[i].query = NULL;
    _abort_buffer_empty_slots = _abort_buffer_size;
    _abort_buffer_enable = (g_params["abort_buffer_enable"] == "true");


    hotspots = new std::set<uint64_t>();
    wound_entry = new std::vector<std::string>();

#if CC_ALG == REBIRTH_RETIRE
    free_list = new std::vector<Version *>();
    total_sz = 900000;
//    for (int i = 0; i < total_sz; ++i) {
//        auto version_reserve = (Version *) _mm_malloc(sizeof(Version), 64);
//        version_reserve->begin_ts = INF;
//        version_reserve->end_ts = INF;
//        version_reserve->dynamic_txn_ts = (volatile ts_t *)_mm_malloc(sizeof(ts_t), 64);
//        version_reserve->dynamic_txn_ts = new ts_t(0);
//        version_reserve->type = XP;
//        version_reserve->read_queue = NULL;
//        version_reserve->prev = NULL;
//        version_reserve->next = NULL;
//        version_reserve->retire = NULL;
//        version_reserve->version_number = 0;
//        version_reserve->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
//        version_reserve->data->init(MAX_TUPLE_SIZE);
//        free_list->push_back(version_reserve);
//    }
#endif
}

uint64_t thread_t::get_thd_id() { return _thd_id; }
uint64_t thread_t::get_host_cid() {	return _host_cid; }
void thread_t::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t thread_t::get_cur_cid() { return _cur_cid; }
void thread_t::set_cur_cid(uint64_t cid) {_cur_cid = cid; }


RC thread_t::run() {
#if !NOGRAPHITE
    _thd_id = CarbonGetTileId();
#endif

    if (warmup_finish) {
        mem_allocator.register_thread(_thd_id);
    }
//    pthread_barrier_wait( &warmup_bar );
    stats.init(get_thd_id());
//    pthread_barrier_wait( &warmup_bar );

    set_affinity(get_thd_id());

    myrand rdm;
    rdm.init(get_thd_id());
    RC rc = RCOK;
    txn_man * m_txn;
    // get txn man from workload
    rc = _wl->get_txn_man(m_txn, this);
    assert (rc == RCOK);
    glob_manager->set_txn_man(m_txn);

    base_query * m_query = NULL;
    uint64_t thd_txn_id = 0;
    UInt64 txn_cnt = 0;
    ts_t txn_starttime = 0;


    while (true) {
        ts_t starttime = get_sys_clock();
        if (WORKLOAD != TEST) {
            if (_abort_buffer_enable) {
                while(true) {
                    m_query = NULL;
                    ts_t curr_time = get_sys_clock();
                    ts_t min_ready_time = UINT64_MAX;

                    // At least one aborted transaction. Update my_query or min_ready_time.
                    if (_abort_buffer_empty_slots < _abort_buffer_size) {
                        for (int i = 0; i < _abort_buffer_size; i++) {
                            // This aborted transaction is ready to rerun.
                            if (_abort_buffer[i].query != NULL && curr_time >= _abort_buffer[i].ready_time) {
                                m_query = _abort_buffer[i].query;
                                m_query->rerun = true;
                                txn_starttime = _abort_buffer[i].starttime;
                                _abort_buffer[i].query = NULL;
                                _abort_buffer_empty_slots ++;
                                break;
                            } else if (_abort_buffer_empty_slots == 0 && _abort_buffer[i].ready_time < min_ready_time) {
                                // Abort_buffer is full, so we must wait until the first aborted transaction is ready.
                                // We need to update min_ready_time.
                                assert(_abort_buffer[i].query != NULL);
                                min_ready_time = _abort_buffer[i].ready_time;
                            }
                        }
                    }

                    // Abort_buffer is full, so we need to wait.(corresponding to the 'else if' branch in line 85)
                    if (m_query == NULL && _abort_buffer_empty_slots == 0) {
                        M_ASSERT(min_ready_time >= curr_time, "min_ready_time=%ld, curr_time=%ld\n", min_ready_time, curr_time);
                        usleep((min_ready_time - curr_time)/1000);
                    } else if (m_query == NULL) {
                        // No transaction is ready and abort buffer isn't full, so we can directly process the next transaction.
                        m_query = query_queue->get_next_query( _thd_id );
                        m_query->rerun = false;
                        m_txn->abort_cnt = 0;
                        assert(m_query);
                        txn_starttime = starttime;
#if CC_ALG == WAIT_DIE || (CC_ALG == WOUND_WAIT && WW_STARV_FREE)
                        m_txn->set_ts(get_next_ts());
#endif
                    }

                    if (m_query)
                        break;
                }
            }else {
                if (rc == RCOK || rc == ERROR) {           // Make TPC-C available for REBIRTH_RETIRE.
                    m_query = query_queue->get_next_query( _thd_id );

                    m_query->rerun = false;
                    m_txn->abort_cnt = 0;
                    assert(m_query);
                    txn_starttime = starttime;
#if CC_ALG == WAIT_DIE || (CC_ALG == WOUND_WAIT && WW_STARV_FREE)
                    m_txn->set_ts(get_next_ts());
#endif
                }
            }
        }
        INC_STATS(_thd_id, time_query, get_sys_clock() - starttime);
        //#if CC_ALG == VLL
        //		_wl->get_txn_man(m_txn, this);
        //#endif

        /**
         * Before executing a new txn, set its txn_id and status
         */
#if (CC_ALG == WOUND_WAIT) && !WW_STARV_FREE
        m_txn->set_ts(get_next_ts());
#elif (CC_ALG == BAMBOO)
        m_txn->set_ts(0);
#elif CC_ALG == WAIT_DIE || (CC_ALG == WOUND_WAIT && WW_STARV_FREE)
        // used for after warmup, since aborted txn keeps original ts
                if (unlikely(m_txn->get_ts() == 0))
                    m_txn->set_ts(get_next_ts());
#elif CC_ALG == REBIRTH_RETIRE
//        m_txn->hotspot_friendly_txn_id = (get_thd_id() << 32) | (get_sys_clock() & 0xffffffff);
//#if DEADLOCK_DETECTION
//        m_txn->InsertWaitingSet(m_txn->get_hotspot_friendly_txn_id());               // Initialize waiting set
//#endif
        m_txn->set_ts(0);
        m_txn->status = RUNNING;
//        m_txn->ready_abort = false;
//        m_txn->rr_semaphore = 0;
        m_txn->parents.clear();
        m_txn->children.clear();
        m_txn->timestamp_v = 0;

//        assert(m_txn->hotspot_friendly_semaphore == 0);
//        assert(m_txn->hotspot_friendly_dependency->empty());
#endif

        m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
        thd_txn_id ++;
        m_txn->wait_latch_time = 0;
        m_txn->wait_passive_retire = 0;

        if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
            || CC_ALG == MVCC
            || CC_ALG == HEKATON
            || CC_ALG == TIMESTAMP)
            m_txn->set_ts(get_next_ts());

        rc = RCOK;

#if CC_ALG == HSTORE
        if (WORKLOAD == TEST) {
            uint64_t part_to_access[1] = {0};
            rc = part_lock_man.lock(m_txn, &part_to_access[0], 1);
        } else
            rc = part_lock_man.lock(m_txn, m_query->part_to_access, m_query->part_num);
#elif CC_ALG == VLL
        vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
        glob_manager->add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
        // In the original OCC paper, start_ts only reads the current ts without advancing it.
                // But we advance the global ts here to simplify the implementation. However, the final
                // results should be the same.
                m_txn->start_ts = get_next_ts();
#endif

#if PF_ABORT
//        m_txn->start_sys_clock = get_sys_clock();
//        m_txn->wound = false;
//        m_txn->wound_cascad = false;
#endif

        if (rc == RCOK)
        {
#if CC_ALG != VLL
            if (WORKLOAD == TEST)
                rc = runTest(m_txn);
            else {
                rc = m_txn->run_txn(m_query);           // Call ycsb_txn_man::run_txn()
            }
#endif

#if CC_ALG == HSTORE
            if (WORKLOAD == TEST) {
                uint64_t part_to_access[1] = {0};
                part_lock_man.unlock(m_txn, &part_to_access[0], 1);
            } else
                part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
#endif
        }

        // 2-15 [BUG in BamBoo]: Wrong time to get endtime, which causes wrong throughput when ABORT_BUFFER_ENABLE == false.
        //ts_t endtime = get_sys_clock();

        if (rc == Abort) {
            uint64_t penalty = 0;
            if (ABORT_PENALTY != 0)  {
                double r;
                drand48_r(&buffer, &r);
                penalty = r * ABORT_PENALTY;
            }
            if (!_abort_buffer_enable)
                usleep(penalty / 1000);
            else {
                assert(_abort_buffer_empty_slots > 0);
                for (int i = 0; i < _abort_buffer_size; i ++) {
                    if (_abort_buffer[i].query == NULL) {
                        _abort_buffer[i].query = m_query;
                        _abort_buffer[i].ready_time = get_sys_clock() + penalty;
                        _abort_buffer[i].starttime = txn_starttime;
                        _abort_buffer_empty_slots --;
                        break;
                    }
                }
            }
        }

        // 2-15 [BUG in BamBoo] : Make throughput correct when ABORT_BUFFER_ENABLE == false.
        ts_t endtime = get_sys_clock();

        uint64_t timespan = endtime - starttime;
        INC_STATS(get_thd_id(), run_time, timespan);
        //stats.add_lat(get_thd_id(), timespan);
        if (rc == RCOK) {
            INC_STATS(get_thd_id(), commit_latency, timespan);
            INC_STATS(get_thd_id(), latency, endtime - txn_starttime);
            INC_STATS(get_thd_id(), txn_cnt, 1);
#if WORKLOAD == YCSB
            if (unlikely(g_long_txn_ratio > 0)) {
                // request_cnt of a long transaction equals to MAX_ROW_PER_TXN >> REQ_PER_QUERY
                if ( ((ycsb_query *) m_query)->request_cnt > REQ_PER_QUERY)
                    INC_STATS(get_thd_id(), txn_cnt_long, 1);
            }
#endif
            stats.commit(get_thd_id());
            auto time_wait_passive = m_txn->wait_passive_retire;
            DEC_STATS(get_thd_id(), time_get_cs, time_wait_passive);
            txn_cnt ++;
        }
        else if (rc == Abort) {
            INC_STATS(get_thd_id(), time_abort, timespan);
            // time abort does not include the lath time and commit time
            auto time_latch_wait = m_txn->wait_latch_time;
            DEC_STATS(get_thd_id(), time_abort, time_latch_wait);
            // time abort does not include the lock wait time
            auto time_wait_tmp = stats.tmp_stats[get_thd_id()]->time_wait;
            DEC_STATS(get_thd_id(), time_abort, time_wait_tmp);
            INC_STATS(get_thd_id(), time_wait, time_wait_tmp);
//#if PF_ABORT
//            if (m_txn->wound){
//                INC_STATS(get_thd_id(), time_wound, timespan);
//            }
//            if (m_txn->wound_cascad){
//                INC_STATS(get_thd_id(), time_wound_cascad, timespan);
//            }
//#endif
            INC_STATS(get_thd_id(), abort_cnt, 1);
#if WORKLOAD == YCSB
            if (unlikely(g_long_txn_ratio > 0)) {
                if ( ((ycsb_query *) m_query)->request_cnt > REQ_PER_QUERY)
                    INC_STATS(get_thd_id(), abort_cnt_long, 1);
            }
#endif
            stats.abort(get_thd_id());
            m_txn->abort_cnt++;
#if WORKLOAD == TPCC
            auto txn_query = (tpcc_query *) m_query;
            auto txn_typ=txn_query->type;
            if (txn_typ == TPCC_NEW_ORDER){
                INC_STATS(get_thd_id(), abort_cnt_neworder, 1);
            }else if(txn_typ == TPCC_PAYMENT){
                INC_STATS(get_thd_id(), abort_cnt_payment, 1);
            }
#endif
        }
        else if (rc == ERROR) {
            // user initiated aborts in TPC-C
            INC_STATS(get_thd_id(), time_abort, timespan);
            INC_STATS(get_thd_id(), user_abort_cnt, 1);
            INC_STATS(get_thd_id(), abort_cnt, 1);
#if WORKLOAD == YCSB
            if (unlikely(g_long_txn_ratio > 0)) {
                printf("Extremely Wrong!\n");
                if ( ((ycsb_query *) m_query)->request_cnt > REQ_PER_QUERY)
                    INC_STATS(get_thd_id(), abort_cnt_long, 1);
            }
#endif
            stats.abort(get_thd_id());
            m_txn->abort_cnt ++;
#if WORKLOAD == TPCC
            auto txn_query = (tpcc_query *) m_query;
            auto txn_typ=txn_query->type;
            if (txn_typ == TPCC_NEW_ORDER){
                INC_STATS(get_thd_id(), abort_cnt_neworder, 1);
            }else if(txn_typ == TPCC_PAYMENT){
                INC_STATS(get_thd_id(), abort_cnt_payment, 1);
            }
#endif
        }

        if (rc == FINISH) {
#if CC_ALG == IC3
            m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
#endif
            return rc;
        }

        // Warmup finish condition.
        if (!warmup_finish && txn_cnt >= WARMUP / g_thread_cnt)
        {
            stats.clear( get_thd_id() );
            return FINISH;
        }

        // Execution finish condition
#if TERMINATE_BY_COUNT
        if (warmup_finish && txn_cnt >= g_txn_max_part) {
            assert(txn_cnt == g_txn_max_part);
            if( !ATOM_CAS(_wl->sim_done, false, true) )
                assert( _wl->sim_done);
        }
#else
        if (warmup_finish && (stats._stats[get_thd_id()]->run_time / 1000000000 >= MAX_RUNTIME)) {
            if( !ATOM_CAS(_wl->sim_done, false, true) )
                assert( _wl->sim_done);
        }
#endif

        if (_wl->sim_done) {
//#if CC_ALG == REBIRTH_RETIRE
//            this->free_versions_mem();
//#endif
#if CC_ALG == IC3
            m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
#endif
            return FINISH;
        }
    }
    assert(false);
}


ts_t thread_t::get_next_n_ts(int n) {
    return glob_manager->get_n_ts(n);
}

ts_t thread_t::get_next_ts() {
    if (g_ts_batch_alloc) {
        if (_curr_ts % g_ts_batch_num == 0) {
            _curr_ts = glob_manager->get_ts(get_thd_id());
            _curr_ts ++;
        } else {
            _curr_ts ++;
        }
        return _curr_ts - 1;
    } else {
#if CC_ALG == REBIRTH_RETIRE
        #if NEXT_TS
        _curr_ts = glob_manager->get_ts(get_thd_id());
        #else
        _curr_ts ++;
        uint64_t time = _curr_ts;
        uint64_t thd_id = get_thd_id();
        uint64_t time_part = time & ((1ULL << 48) - 1);  // 低 48 位
        uint64_t thd_id_part = thd_id & ((1ULL << 16) - 1);  // 低 16 位
        // 将 time 的低 48 位移到高 16 位位置，并加上 thd_id 的低 16 位
        uint64_t timestamp = (time_part << 16) | thd_id_part;
        _curr_ts = timestamp;
        #endif
#else
        _curr_ts = glob_manager->get_ts(get_thd_id());
#endif
        return _curr_ts;
    }
}

RC thread_t::runTest(txn_man * txn)
{
    RC rc = RCOK;
    if (g_test_case == READ_WRITE) {
        rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
#if CC_ALG == OCC
        txn->start_ts = get_next_ts();
#endif
        rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 1);
        printf("READ_WRITE TEST PASSED\n");
        return FINISH;
    }
    else if (g_test_case == CONFLICT) {
        rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
        if (rc == RCOK)
            return FINISH;
        else
            return rc;
    }
    assert(false);
    return RCOK;
}

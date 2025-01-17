#include <sched.h>
#include "query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"

/*************************************************/
//     class Query_queue
/*************************************************/
int Query_queue::_next_tid;

void Query_queue::init(workload * h_wl) {
    all_queries = new Query_thd * [g_thread_cnt];
    _wl = h_wl;
    _next_tid = 0;

#if WORKLOAD == YCSB
//    ycsb_query::init_var();
    ycsb_query::calculateDenom();
#elif WORKLOAD == TPCC
    assert(tpcc_buffer != NULL);
#endif

    int64_t begin = get_server_clock();
    pthread_t p_thds[g_thread_cnt - 1];
    for (UInt32 i = 0; i < g_thread_cnt - 1; i++) {
        pthread_create(&p_thds[i], NULL, threadInitQuery, this);
    }
    threadInitQuery(this);
    for (uint32_t i = 0; i < g_thread_cnt - 1; i++)
        pthread_join(p_thds[i], NULL);
    int64_t end = get_server_clock();
    printf("Query Queue Init Time %f\n", 1.0 * (end - begin) / 1000000000UL);
}

void Query_queue::init_per_thread(int thread_id) {
    all_queries[thread_id] = (Query_thd *) _mm_malloc(sizeof(Query_thd), 64);
    all_queries[thread_id]->init(_wl, thread_id);
}

base_query * Query_queue::get_next_query(uint64_t thd_id) {
    base_query * query = all_queries[thd_id]->get_next_query();
    return query;
}

void * Query_queue::threadInitQuery(void * This) {
    Query_queue * query_queue = (Query_queue *)This;
    uint32_t tid = ATOM_FETCH_ADD(_next_tid, 1);

    // set cpu affinity
    set_affinity(tid);

    query_queue->init_per_thread(tid);
    return NULL;
}

/*************************************************/
//     class Query_thd
/*************************************************/


void Query_thd::init(workload * h_wl, int thread_id) {
    q_idx = 0;

#if TPCC_USER_ABORT
    request_cnt = WARMUP / g_thread_cnt + g_txn_max_part + 100 + g_txn_max_part / 100;
#else
    request_cnt = WARMUP / g_thread_cnt + g_txn_max_part + 100;
#endif

#if ABORT_BUFFER_ENABLE
    request_cnt += ABORT_BUFFER_SIZE;
#endif

    // txn count
//        request_cnt = 20;

#if WORKLOAD == YCSB
    queries = (ycsb_query *)
            mem_allocator.alloc(sizeof(ycsb_query) * request_cnt, thread_id);

    // Set the random seed of drand48_r(). The random seed of every thread is different. But the random seed may be the same of different txns created and executed by the same thread.
    srand48_r(thread_id + 1, &buffer);

    // XXX(zhihan): create a pre-allocated space for long txn
    if (g_long_txn_ratio > 0) {
        long_txn = (ycsb_request *)
                mem_allocator.alloc(sizeof(ycsb_request) * MAX_ROW_PER_TXN, thread_id);
        long_txn_part = (uint64_t *)
                mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, thread_id);
    }
#elif WORKLOAD == TPCC
    queries = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * request_cnt, 64);
#endif

    for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB
        new(&queries[qid]) ycsb_query();
        queries[qid].init(thread_id, h_wl, this);
#elif WORKLOAD == TPCC
        new(&queries[qid]) tpcc_query();
                queries[qid].init(thread_id, h_wl);
#endif
    }
}


base_query *
Query_thd::get_next_query() {
    if (q_idx >= request_cnt-1) {
        q_idx = 0;
        assert(q_idx < request_cnt);
        //printf("WARNING: run out of queries, increase txn cnt per part!\n");
        //return NULL;
    }
    base_query * query = &queries[q_idx++];
    return query;
}

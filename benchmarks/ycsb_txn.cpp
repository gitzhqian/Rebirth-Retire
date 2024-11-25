#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "thread"
#include "row_rr.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
    txn_man::init(h_thd, h_wl, thd_id);
    _wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
    RC rc;
    ycsb_query * m_query = (ycsb_query *) query;
    ycsb_wl * wl = (ycsb_wl *) h_wl;
    itemid_t * m_item = NULL;

#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
    int access_id;
    retire_threshold = (uint32_t) floor(m_query->request_cnt * (1 - g_last_retire));
#elif CC_ALG != REBIRTH_RETIRE
    row_cnt = 0;
#endif

    // 2-27 Optimization for read_only long transaction.
#if CC_ALG == REBIRTH_RETIRE && READ_ONLY_OPTIMIZATION_ENABLE
    is_long = m_query->is_long;
    if(m_query->local_read_perc == 1){
        read_only = true;
    }
#endif


    // if long txn and not rerun aborted txn, generate queries
    if (unlikely(m_query->is_long && !(m_query->rerun))) {
        uint64_t starttime = get_sys_clock();
        m_query->gen_requests(h_thd->get_thd_id(), h_wl);
        DEC_STATS(h_thd->get_thd_id(), run_time, get_sys_clock() - starttime);
    }


    for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
#if CC_ALG == REBIRTH_RETIRE
        // REBIRTH_RETIRE: Abort txn actively(before executing next operation)
        if(this->status == ABORTED ){
//            printf("ycsb executing rc: %d. \n",rc);
//            INC_STATS(this->get_thd_id(), find_circle_abort, 1);
            rc = Abort;
            goto final;
        }
#endif

        ycsb_request * req = &m_query->requests[rid];
        int part_id = wl->key_to_part( req->key );
        bool finish_req = false;
        UInt32 iteration = 0;

        while ( !finish_req ) {
            if (iteration == 0) {
                m_item = index_read(_wl->the_index, req->key, part_id);
                // ADD: there can be no corresponding data
                // 2-27 : cur_node will never be NULL, because YCSB always access an existed tuple.
//                if (m_item == NULL)
//                    break;
            }

#if INDEX_STRUCT == IDX_BTREE
            else {
                _wl->the_index->index_next(get_thd_id(), m_item);
                if (m_item == NULL)
                    break;
            }
#endif

            row_t * row = ((row_t *)m_item->location);
            row_t * row_local;
            access_t type = req->rtype;
            //printf("[txn-%lu] start %d requests at key %lu\n", get_txn_id(), rid, req->key);
            row_local = get_row(row, type);         // Call txn_man::get_row()
            if (row_local == NULL) {                // NULL: means the access fails
//                printf("error. \n");
                rc = Abort;
                goto final;
            }
#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
            access_id = row_cnt - 1;
#endif

            // Computation //
            // Only do computation when there are more than 1 requests.
//#if PF_CS
//            uint64_t rd_wrt_time = get_sys_clock();
//#endif
            if (m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
//                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
                    int fid = 0;
                    char * data = row_local->get_data();
                    __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
//                  }
                } else {
                    assert(req->rtype == WR);
//					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
                    int fid = 0;

                    /*
                     * [BUG in DBx1000]: Transaction should modify the ""row_local"" returned by get_row(), which is a new version in Hekaton and a local data copy in Silo and TicToc.
                     * We shouldn't modify the "row" because it's the tuple that is visible to other transactions.
                     * However, since "row_t" only stores the value, if we really modify "row", it won't affect the correctness of CC, because CC only uses locks and accesses.
                     */
//#if (CC_ALG == BAMBOO) || (CC_ALG == WOUND_WAIT)
                    char * data = row_local->get_data();
//#else
//                        char * data = row->get_data();
//#endif
                    /*
                     * Note that the expression "&data[fid * 10]" is used to access the memory address of the "fid"th element in the "data" array, where each element is assumed to be 10 bytes in size.
                     * The "(uint64_t *)" typecast is used to interpret this memory address as a pointer to an unsigned 64-bit integer, which is then dereferenced and assigned the value 0.
                     * This effectively sets the 8 bytes starting from the "fid"th position in the "data" array to 0.
                     */
                    // The real modification of an update operation.
                    *(uint64_t *)(&data[fid * 10]) = 0;
//					}
                }
            }
//#if PF_CS
//            INC_STATS(this->get_thd_id(), time_read_write, get_sys_clock() - rd_wrt_time);
//#endif

            iteration ++;
            if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
                finish_req = true;

#if (CC_ALG == BAMBOO) && (THREAD_CNT != 1)
            // retire write txn
            if (finish_req && (req->rtype == WR) && (rid <= retire_threshold)) {
                //printf("[txn-%lu] retire %d requests\n", get_txn_id(), rid);
                if (retire_row(access_id) == Abort)
                    return finish(Abort);
            }
#endif
        }
    }

    rc = RCOK;
    final:

#if CC_ALG == REBIRTH_RETIRE
    if (rc == RCOK){
//        printf("ycsb final rc: %d. \n",rc);
        auto ret = ATOM_CAS(status, RUNNING, validating);
//        assert(ret);
    }
    if (rc == Abort) {
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
//        assert(ret);

    }
#endif

    rc = finish(rc);
    return rc;
}


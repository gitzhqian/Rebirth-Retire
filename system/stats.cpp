#include <iostream>
#include <fstream>
#include <iomanip>
#include "global.h"
#include "helper.h"
#include "stats.h"
#include "mem_alloc.h"

#define BILLION 1000000000UL

/**
 * [Stats_thd] ================================================================================
 */

/**
*
* @param thd_id : UNUSED
*/
void Stats_thd::init(uint64_t thd_id) {
    clear();
    all_debug1 = (uint64_t *)_mm_malloc(sizeof(uint64_t) * g_txn_max_part, 64);
    all_debug2 = (uint64_t *)_mm_malloc(sizeof(uint64_t) * g_txn_max_part, 64);
}

// Initialize all metrics declared in Stats_thd class.
void Stats_thd::clear() {
    ALL_METRICS(INIT_VAR, INIT_VAR, INIT_VAR)
}



/**
 * [Stats_tmp] ================================================================================
 */
void Stats_tmp::init() {
    clear();
}

// Initialize 3 metrics declared in Stats_tmp class.
void Stats_tmp::clear() {
    TMP_METRICS(INIT_VAR, INIT_VAR)
}


/**
 * [Stats] ====================================================================================
 */

/**
 * 1. Malloc space for _stats and tmp_stats of all threads.
 * 2. Initialize GLOBAL metrics.
 */
void Stats::init() {
    if (!STATS_ENABLE)              // STATS_ENABLE == true in config.h
        return;

    // Malloc space for PER_THREAD metrics.
    _stats = (Stats_thd**)_mm_malloc(sizeof(Stats_thd*) * g_thread_cnt, 64);
    tmp_stats = (Stats_tmp**)_mm_malloc(sizeof(Stats_tmp*) * g_thread_cnt, 64);

    // Initialize GLOBAL metrics.
    dl_detect_time = 0;
    dl_wait_time = 0;
    deadlock = 0;
    cycle_detect = 0;

    created_long_txn_cnt = (int*)_mm_malloc(sizeof(int) * g_thread_cnt, 64);
    for(int i = 0; i < g_thread_cnt;i++){
        created_long_txn_cnt[i] = 0;
    }

}

/**
 * 1. Malloc space for _stats and tmp_stats object of a specific thread.
 * 2. Initialize all metrics of the specific thread.
 * @param thread_id : id of the specific thread
 */
void Stats::init(uint64_t thread_id) {
    if (!STATS_ENABLE)
        return;

    _stats[thread_id] = (Stats_thd *)_mm_malloc(sizeof(Stats_thd), 64);
    tmp_stats[thread_id] = (Stats_tmp *)_mm_malloc(sizeof(Stats_tmp), 64);

    _stats[thread_id]->init(thread_id);
    tmp_stats[thread_id]->init();
}

/**
 * Clear PER_THREAD and GLOBAL metrics of a specific thread.
 * @param tid : id of the specific thread
 */
void Stats::clear(uint64_t tid) {
    if (STATS_ENABLE) {
        _stats[tid]->clear();
        tmp_stats[tid]->clear();

        dl_detect_time = 0;
        dl_wait_time = 0;
        cycle_detect = 0;
        deadlock = 0;
    }
}

/**
 * USELESS(only TICTOC calls it)
 * g_prt_lat_distr: print the transaction latency distribution or not.
 * Since g_prt_lat_distr == false in config.h, this function does nothing.
 */
void Stats::add_debug(uint64_t thd_id, uint64_t value, uint32_t select) {
    if (g_prt_lat_distr && warmup_finish) {           // g_prt_lat_distr == false
        uint64_t tnum = _stats[thd_id]->txn_cnt;
        if (select == 1)
            _stats[thd_id]->all_debug1[tnum] = value;
        else if (select == 2)
            _stats[thd_id]->all_debug2[tnum] = value;
    }
}

/**
 * The transaction in thread(thd_id) commits successfully.
 * Record 3 processing metrics in tmp_stats[thd_id] of this transaction to _stats[thd_id].
 * @param thd_id : thread_ID of a thread which just commits a transaction
 */
void Stats::commit(uint64_t thd_id) {
    if (STATS_ENABLE) {
        _stats[thd_id]->time_man += tmp_stats[thd_id]->time_man;
        _stats[thd_id]->time_index += tmp_stats[thd_id]->time_index;
        _stats[thd_id]->time_wait += tmp_stats[thd_id]->time_wait;
        _stats[thd_id]->time_copy += tmp_stats[thd_id]->time_copy;
        tmp_stats[thd_id]->init();
    }
}

/**
 * The transaction in thread(thd_id) aborts.
 * Clear 3 processing metrics in tmp_stats[thd_id] of this transaction.
 * @param thd_id
 */
void Stats::abort(uint64_t thd_id) {
    if (STATS_ENABLE)
        tmp_stats[thd_id]->init();
}

/**
 * Calculate processing results_background of this workload.
 */
void Stats::print() {
    // Declare and initialize total_XX metrics for every metric.
    ALL_METRICS(INIT_TOTAL_VAR, INIT_TOTAL_VAR, INIT_TOTAL_VAR)

    // Calculate the sum of x&y metrics and the max of z metric.
//    for (uint64_t tid = 0; tid < g_thread_cnt; tid ++) {
//        ALL_METRICS(SUM_UP_STATS, SUM_UP_STATS, MAX_STATS)
//        // print the proccessing result of each thread to terminal
//        printf("[tid=%lu] txn_cnt=%lu,abort_cnt=%lu, user_abort_cnt=%lu, txn_cnt_long=%lu,abort_cnt_long=%lu, created_long_txn_cnt=%d\n",
//            tid, _stats[tid]->txn_cnt, _stats[tid]->abort_cnt,
//            _stats[tid]->user_abort_cnt, _stats[tid]->txn_cnt_long, _stats[tid]->abort_cnt_long,  created_long_txn_cnt[tid]);
//    }

    // Calculate the average value.
    // Todo: Notice that the value of these three metrics(latency,commit_latency,time_man) isn't divided by BILLION,
    //  because it is already very small. Once we divide it, they will all equal to 0.
//    total_latency = total_latency / total_txn_cnt;
//    total_commit_latency = total_commit_latency / total_txn_cnt;
//    total_time_man = total_time_man - total_time_wait;

    if (output_file != NULL) {
        ofstream outf(output_file,ios::app);

        if (outf.is_open()) {
            for (uint64_t tid = 0; tid < g_thread_cnt; tid ++) {
                ALL_METRICS(SUM_UP_STATS, SUM_UP_STATS, MAX_STATS)
                // print the proccessing result of each thread to terminal
                outf << "[tid=" << tid << "] ";
                outf << "txn_cnt=" << _stats[tid]->txn_cnt << ", ";
                outf << "abort_cnt=" << _stats[tid]->abort_cnt << ", ";
                outf << "user_abort_cnt=" << _stats[tid]->user_abort_cnt << ", ";
                outf << "txn_cnt_long=" << _stats[tid]->txn_cnt_long << ", ";
                outf << "abort_cnt_long=" << _stats[tid]->abort_cnt_long << ", ";
                outf << "created_long_txn_cnt=" << created_long_txn_cnt[tid] << ", ";
                outf << "abort_hotspot=" << _stats[tid]->abort_hotspot << ", ";
                outf << "abort_position=" << _stats[tid]->abort_position << ", ";
                outf << "abort_position_cnt=" << _stats[tid]->abort_position_cnt << ", " "\n";
            }

            total_latency = total_latency / total_txn_cnt;
            total_commit_latency = total_commit_latency / total_txn_cnt;
            total_time_man = total_time_man - total_time_wait;

            outf << "[summary] throughput=" << total_txn_cnt / total_run_time * BILLION * THREAD_CNT << ", ";
            outf << "abort_rate=" << (double)total_abort_cnt / (double)(total_abort_cnt + total_txn_cnt) << ", ";
            ALL_METRICS(WRITE_STAT_X, WRITE_STAT_Y, WRITE_STAT_Y)
            outf << "deadlock_cnt=" << deadlock << ", ";
            outf << "cycle_detect=" << cycle_detect << ", ";
            outf << "dl_detect_time=" << dl_detect_time / BILLION << ", ";
            outf << "dl_wait_time=" << dl_wait_time / BILLION << "\n\n";
            outf.precision(10);
            outf.close();
        }
    }else {

        for (uint64_t tid = 0; tid < g_thread_cnt; tid ++) {
            ALL_METRICS(SUM_UP_STATS, SUM_UP_STATS, MAX_STATS)
            // print the proccessing result of each thread to terminal
            printf("[tid=%lu] txn_cnt=%lu,abort_cnt=%lu, user_abort_cnt=%lu, txn_cnt_long=%lu,abort_cnt_long=%lu, created_long_txn_cnt=%d,abort_hotspot=%lu,abort_position=%lu,abort_position_cnt=%lu\n",
                   tid, _stats[tid]->txn_cnt, _stats[tid]->abort_cnt,
                   _stats[tid]->user_abort_cnt, _stats[tid]->txn_cnt_long, _stats[tid]->abort_cnt_long,  created_long_txn_cnt[tid], _stats[tid]->abort_hotspot, _stats[tid]->abort_position, _stats[tid]->abort_position_cnt);
        }

        total_latency = total_latency / total_txn_cnt;
        total_commit_latency = total_commit_latency / total_txn_cnt;
        total_time_man = total_time_man - total_time_wait;

        std::cout << "[summary] throughput=" << total_txn_cnt / total_run_time * BILLION * THREAD_CNT << ", ";
        std::cout << "abort_rate=" << (double)total_abort_cnt / (double)(total_abort_cnt + total_txn_cnt) << ", ";
        ALL_METRICS(PRINT_STAT_X, PRINT_STAT_Y, PRINT_STAT_Y)
        std::cout << "deadlock_cnt=" << deadlock << ", ";
        std::cout << "cycle_detect=" << cycle_detect << ", ";
        std::cout << "dl_detect_time=" << dl_detect_time / BILLION << ", ";
        std::cout << "dl_wait_time=" << dl_wait_time / BILLION << "\n";

    }
    if (g_prt_lat_distr)      // g_prt_lat_distr == false
        print_lat_distr();
}



/**
 * Print the transaction latency distribution.
 */
void Stats::print_lat_distr() {
    FILE * outf;
    if (output_file != NULL) {
        outf = fopen(output_file, "a");
        for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) {
            fprintf(outf, "[all_debug1 thd=%d] ", tid);
            for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++)
                fprintf(outf, "%ld,", _stats[tid]->all_debug1[tnum]);
            fprintf(outf, "\n[all_debug2 thd=%d] ", tid);
            for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++)
                fprintf(outf, "%ld,", _stats[tid]->all_debug2[tnum]);
            fprintf(outf, "\n");
        }
        fclose(outf);
    }
}

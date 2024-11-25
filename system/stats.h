#pragma once

#define TMP_METRICS(x, y) \
  x(double, time_wait) x(double, time_man) x(double, time_index) x(double, time_copy)
#define ALL_METRICS(x, y, z) \
  y(uint64_t, txn_cnt) y(uint64_t, abort_cnt) y(uint64_t, user_abort_cnt) \
  y(uint64_t, abort_cnt_neworder) y(uint64_t, abort_cnt_payment)  \
  x(double, run_time) x(double, time_abort) x(double, time_cleanup)       \
  x(double, time_lockrow) x(double, time_exec) x(double, time_assign) x(double, time_rebirth) x(double, time_creat_entry) \
  x(double, time_creat_version) x(double, time_read_write) x(double, time_verify)  \
  x(double, time_wound) x(double, time_wound_cascad) \
  x(double, time_abort_processing) x(double, time_commit_processing) \
  x(double, time_query) x(double, time_get_latch) x(double, time_get_cs)  \
  x(double, time_copy_latch)  x(double, time_retire_latch) x(double, time_retire_cs) \
  x(double, time_release_latch) x(double, time_release_cs) x(double, time_semaphore_cs) \
  x(double, time_commit)  x(double, total_read_wait) x(double, total_write_wait)   \
  y(uint64_t, abort_position) y(uint64_t, abort_position_cnt) y(uint64_t, abort_hotspot) \
  y(uint64_t, time_ts_alloc) y(uint64_t, wait_cnt) \
  y(uint64_t, latency) y(uint64_t, commit_latency) y(uint64_t, abort_length) \
  y(uint64_t, cascading_abort_times) z(uint64_t, max_abort_length) \
  y(uint64_t, find_circle_abort_depent)\
  y(uint64_t, txn_cnt_long) y(uint64_t, abort_cnt_long) y(uint64_t, cascading_abort_cnt) \
  y(uint64_t, lock_acquire_cnt) y(uint64_t, lock_directly_cnt) y(uint64_t, blind_kill_count)  \
  TMP_METRICS(x, y)

//y(uint64_t, wound_times)  y(uint64_t, wound_depend_cnt)  y(uint64_t, wound_depent_cnt) \
//  y(uint64_t, wound_retire_cnt)  y(uint64_t, wound_wait_cnt)   \

#define DECLARE_VAR(tpe, name) tpe name;
#define INIT_VAR(tpe, name) name = 0;
#define INIT_TOTAL_VAR(tpe, name) tpe total_##name = 0;
#define SUM_UP_STATS(tpe, name) total_##name += _stats[tid]->name;
#define MAX_STATS(tpe, name) total_##name = max(total_##name, _stats[tid]->name);

#define STR(x) #x
#define XSTR(x) STR(x)
#define STR_X(tpe, name) XSTR(name)
#define VAL_X(tpe, name) total_##name / BILLION         // VAL_X: calculate the metrics that is relevant to time, so it needs to be divided by billion.
#define VAL_Y(tpe, name) total_##name

#define PRINT_STAT_X(tpe, name) \
  std::cout << STR_X(tpe, name) << "= " << VAL_X(tpe, name) << ", ";
#define PRINT_STAT_Y(tpe, name) \
  std::cout << STR_X(tpe, name) << "= " << VAL_Y(tpe, name) << ", ";
#define WRITE_STAT_X(tpe, name) \
  outf << STR_X(tpe, name) << "= " << VAL_X(tpe, name) << ", ";
#define WRITE_STAT_Y(tpe, name) \
  outf << STR_X(tpe, name) << "= " << VAL_Y(tpe, name) << ", ";

class Stats_thd {
public:
    void init(uint64_t thd_id);
    void clear();

    char _pad2[CL_SIZE];

    // Declare all metrics(except for metrics for statistic purpose, eg. total_XX)
    ALL_METRICS(DECLARE_VAR, DECLARE_VAR, DECLARE_VAR)

    uint64_t * all_debug1;
    uint64_t * all_debug2;
    char _pad[CL_SIZE];
};

class Stats_tmp {
public:
    void init();
    void clear();

    // temporary stats only include 3 metrics
    double time_man;
    double time_index;
    double time_wait;
    double time_copy;

    char _pad[CL_SIZE - sizeof(double)*3];
};

class Stats {
public:
    // PER THREAD statistics
    Stats_thd ** _stats;
    // stats are first written to tmp_stats, if the txn successfully commits,
    // copy the values in tmp_stats to _stats
    Stats_tmp ** tmp_stats;

    // GLOBAL statistics
    double dl_detect_time;
    double dl_wait_time;
    uint64_t cycle_detect;
    uint64_t deadlock;

    int * created_long_txn_cnt;

    void init();
    void init(uint64_t thread_id);
    void clear(uint64_t tid);
    void add_debug(uint64_t thd_id, uint64_t value, uint32_t select);
    void commit(uint64_t thd_id);
    void abort(uint64_t thd_id);
    void print();
    void print_lat_distr();
};

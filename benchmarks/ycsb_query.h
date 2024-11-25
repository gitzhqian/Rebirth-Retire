#ifndef _YCSB_QUERY_H_
#define _YCSB_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;
class Query_thd;
// Each ycsb_query contains several ycsb_requests, 
// each of which is a RD, WR to a single table

class ycsb_request {
public:
    access_t rtype;
    uint64_t key;
    char value;
    // only for (qtype == SCAN)
    UInt32 scan_len;
};

class ycsb_query : public base_query {
public:
    void init(uint64_t thd_id, workload * h_wl) { assert(false); };
    void init(uint64_t thd_id, workload * h_wl, Query_thd * query_thd);
    static void calculateDenom();
    static void init_var(){
        the_n = 0;
        denom = 0;
    };
    uint64_t get_new_row();
    void gen_requests(uint64_t thd_id, workload * h_wl);

    uint64_t request_cnt;
    uint64_t local_req_per_query;
    bool is_long;
    double local_read_perc;
    ycsb_request * requests;

#if TEST_BB_ABORT
    std::vector<std::pair<uint64_t,bool>> hotspot_set;
#endif

private:
    // for Zipfian distribution
    static double zeta(uint64_t n, double theta);
    uint64_t zipf(uint64_t n, double theta);

    static uint64_t the_n;
    static double denom;
    double zeta_2_theta;
    Query_thd * _query_thd;
};

#endif

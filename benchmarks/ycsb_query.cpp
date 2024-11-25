#include <random>
#include "query.h"
#include "ycsb_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb.h"
#include "table.h"

uint64_t ycsb_query::the_n = 0;
double ycsb_query::denom = 0;

/**
 * Long txn : Set is_long=true and return.
 * Short txn : Generate requests.
 * @param thd_id
 * @param h_wl
 * @param query_thd
 */
void ycsb_query::init(uint64_t thd_id, workload * h_wl, Query_thd * query_thd) {
    _query_thd = query_thd;
    local_read_perc = g_read_perc;
    local_req_per_query = REQ_PER_QUERY;

    //[BUG in BAMBOO]: The original x is not a qualified random data.
    //	double x = (double)(rand() % 100) / 100.0;
    double x;
    drand48_r(&_query_thd->buffer, &x);

    if (x < g_long_txn_ratio) {
        local_req_per_query = MAX_ROW_PER_TXN;              // assign the count of requests as MAX_ROW_PER_TXN
        local_read_perc = g_long_txn_read_ratio;
        // XXX(zhihan): point requests and part to access to a pre-allocate
        // spot in the thd's query queue; so that dynamically generate queries.
#if WORKLOAD == YCSB
        requests = query_thd->long_txn;
        part_to_access = query_thd->long_txn_part;
#endif
        is_long = true; // used to identify long txn
        zeta_2_theta = zeta(2, g_zipf_theta);

        //2-27 A new auxiliary metric.
        stats.created_long_txn_cnt[thd_id]++;

        return;
    } else {
        requests = (ycsb_request *)mem_allocator.alloc(sizeof(ycsb_request) * local_req_per_query, thd_id);
        part_to_access = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, thd_id);
        is_long = false;
    }
    zeta_2_theta = zeta(2, g_zipf_theta);
    assert(the_n != 0);
    assert(denom != 0);
    gen_requests(thd_id, h_wl);
}


void ycsb_query::calculateDenom()
{
    assert(the_n == 0);
    uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
    the_n = table_size - 1;
    denom = zeta(the_n, g_zipf_theta);
}


// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug.
// The original paper says zeta(theta, 2.0). But I guess it should be zeta(2.0, theta).
double ycsb_query::zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++)
        sum += pow(1.0 / i, theta);
    return sum;
}


uint64_t ycsb_query::zipf(uint64_t n, double theta) {
#if !SYNTHETIC_YCSB
    assert(this->the_n == n);
#endif
    assert(theta == g_zipf_theta);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) /
                 (1 - zeta_2_theta / zetan);
    double u;
    drand48_r(&_query_thd->buffer, &u);
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, theta)) return 2;
    return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}


/**
 * YCSB: Randomly get a row_id(primary index value) of a tuple to be accessed. [range = table_size - 1]
 * Synthetic YCSB: Randomly get a row_id(primary index value) of a tuple to be accessed. [range = table_size - NUM_HS - 1]
 * @return
 */
uint64_t ycsb_query::get_new_row() {
    uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
#if SYNTHETIC_YCSB
    uint64_t row_id = zipf(table_size - NUM_HS - 1, g_zipf_theta);
#else
    uint64_t row_id = zipf(table_size - 1, g_zipf_theta);
#endif

    assert(row_id < table_size);
    return row_id;
}


/**
 * Generate requests for a txn.
 * YCSB:
 * Synthetic_YCSB:
 * @param thd_id
 * @param h_wl
 */
void ycsb_query::gen_requests(uint64_t thd_id, workload * h_wl) {
#if CC_ALG == HSTORE
    assert(g_virtual_part_cnt == g_part_cnt);
#endif

    // Generate the attributes to access(only 1 attribute will be accessed)
    int access_cnt = 0;
    set<uint64_t> all_keys;
    part_num = 0;
    double r = 0;
    int64_t rint64 = 0;
    drand48_r(&_query_thd->buffer, &r);         // r can only be between [0.0,1.0), so only if branch will be processed.
    lrand48_r(&_query_thd->buffer, &rint64);
    if (r < g_perc_multi_part) {            // g_perc_multi_part = g_part_per_txn = 1
        for (UInt32 i = 0; i < g_part_per_txn; i++) {
            if (i == 0 && FIRST_PART_LOCAL)
                // Since g_virtual_part_cnt = 1,so part_to_access[i] = 0.
                part_to_access[part_num] = thd_id % g_virtual_part_cnt;
            else {
                part_to_access[part_num] = rint64 % g_virtual_part_cnt;
            }

            // Guarantee all parts/attributes to access are unique.
            UInt32 j;
            for (j = 0; j < part_num; j++)
                if ( part_to_access[part_num] == part_to_access[j] )
                    break;
            if (j == part_num)
                part_num ++;
        }
    } else {
        part_num = 1;
        if (FIRST_PART_LOCAL)
            // Since g_part_cnt = 1,so part_to_access[i] = 0.
            part_to_access[0] = thd_id % g_part_cnt;
        else
            part_to_access[0] = rint64 % g_part_cnt;
    }

#if SYNTHETIC_YCSB
    uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;              // g_virtual_part_cnt = 1
#endif

    // Initialize local_req_per_query requests of a query/txn.
    uint64_t rid = 0;
    uint64_t tmp;

    // Calculate the random place of hotspot operation in a transaction.
    double temp_hotspot_position;
    int hotspot_position;
    drand48_r(&_query_thd->buffer, &temp_hotspot_position);
    hotspot_position = temp_hotspot_position*local_req_per_query;

    double temp_hotspot_position1;
    int hotspot_position1;
    drand48_r(&_query_thd->buffer, &temp_hotspot_position1);
    hotspot_position1 = temp_hotspot_position1*local_req_per_query;

    double temp_hotspot_position2;
    int hotspot_position2;
    drand48_r(&_query_thd->buffer, &temp_hotspot_position2);
    hotspot_position2 = temp_hotspot_position2*local_req_per_query;

    double temp_hotspot_position3;
    int hotspot_position3;
    drand48_r(&_query_thd->buffer, &temp_hotspot_position3);
    hotspot_position3 = temp_hotspot_position3*local_req_per_query;

    double random_read_ratio ;
#if RANDOM_READ_SYNTHETIC_YCSB
    random_read_ratio = (double)(rand() % 100) / 100.0;
#else
    random_read_ratio = READ_HOTSPOT_RATIO;
#endif

#if SYNTHETIC_YCSB
    uint64_t hs1_row_id = table_size - 1;
    uint64_t hs2_row_id = table_size - 2;
    uint64_t hs3_row_id = table_size - 3;
    uint64_t hs4_row_id = table_size - 4;
#endif

    for (tmp = 0; tmp < local_req_per_query; tmp ++) {
        assert(tmp == rid);
        ycsb_request * req = &requests[rid];
        // the request will access part_id.
        //uint64_t ith = tmp * part_num / g_req_per_query;
        uint64_t ith = tmp * part_num / local_req_per_query;        // part_num = 1, so tmp * part_num is always less than local_req_per_query, so ith is always 0.
        uint64_t part_id = part_to_access[ ith ];           // part_id of all requests is equal to 0.
        uint64_t row_id;

#if SYNTHETIC_YCSB
        assert(part_id == 0);
            #if NUM_HS == 1         // [Single Hotspot]
              #if POS_HS == RANDOM
                if (tmp == hotspot_position) {
                    // insert hotpost at the beginning
//                    req->rtype = FIRST_HS;
                    row_id = table_size - 1;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #elif POS_HS == TOP
                if (tmp == 0) {
                    // insert hotpost at the beginning
//                    req->rtype = FIRST_HS;
                    row_id = table_size - 1;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #elif POS_HS == MID
                if (tmp == (g_req_per_query / 2)) {
                    // insert hotpost at the middle
//                    req->rtype = FIRST_HS;
                    row_id = table_size - 1;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #elif POS_HS == BOT
                if (tmp == (g_req_per_query - 1)) {
                    // insert hotpost at the bottom
//                    req->rtype = FIRST_HS;
                    row_id = table_size - 1;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #elif POS_HS == SPECIFIED
                UInt32 hs_idx = (UInt32) min((int)g_req_per_query-1, max(1, (int) floor(g_req_per_query * g_specified_ratio)));
                if (tmp == hs_idx) {
//                    req->rtype = FIRST_HS;
                    row_id = table_size - 1;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #else
                assert(false);
            #endif
            #elif NUM_HS == 2
                uint64_t hs1_row_id = table_size - 1;
                uint64_t hs2_row_id = table_size - 2;
                double flip;
                drand48_r(&_query_thd->buffer, &flip);
                if (flip < FLIP_RATIO) {
                    hs1_row_id = table_size - 2;
                    hs2_row_id = table_size - 1;
                }
              #if POS_HS == TM
                if (tmp == 0) {
                    // insert hotpost at the beginning
//                    req->rtype = FIRST_HS;
                    row_id = hs1_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else if (tmp == (g_req_per_query / 2)) {
//                    req->rtype = SECOND_HS;
                    row_id = hs2_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #elif POS_HS == MB
                if (tmp == (g_req_per_query / 2)) {
                    // insert hotpost at the middle
//                    req->rtype = FIRST_HS;
                    row_id = hs1_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else if (tmp == (g_req_per_query - 1)) {
//                    req->rtype = SECOND_HS;
                    row_id = hs2_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #elif POS_HS == SPECIFIED
               #if FIXED_HS == 0       // Fix one hotspot to TOP, and modify another hotspot
                UInt32 hs2_idx = (UInt32) min((int)g_req_per_query-1, max(1, (int) floor(g_req_per_query * g_specified_ratio)));
                UInt32 hs1_idx = 0;
               #else       // Fix one hotspot to BOT
                UInt32 hs2_idx = (UInt32) min((int)g_req_per_query-2, max(0, (int) floor(g_req_per_query * (1-g_specified_ratio))));
                UInt32 hs1_idx = g_req_per_query - 1;
               #endif
                if (tmp == hs1_idx) {
                    // insert hotpost at the beginning
//                    req->rtype = FIRST_HS;
                    row_id = hs1_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else if (tmp == hs2_idx) {
//                    req->rtype = SECOND_HS;
                    row_id = hs2_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #elif POS_HS == RANDOM
                if (tmp == hotspot_position) {
                    // insert hotpost at the beginning
                    row_id = hs1_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else if (tmp == hotspot_position1){
                    // insert hotpost at the beginning
                    row_id = hs2_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
              #else
                assert(false);
            #endif
            #elif NUM_HS == 3
                #if POS_HS == SPECIFIED
                double flip;
                drand48_r(&_query_thd->buffer, &flip);
                if (flip < g_flip_ratio) {
                    hs1_row_id = table_size - 2;
                    hs2_row_id = table_size - 1;
                }
                if (tmp == 0) {
//                    req->rtype = FIRST_HS;
                    row_id = hs2_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                }
                else if (tmp == (g_req_per_query / 2)) {
//                    req->rtype = FIRST_HS;
                    row_id = hs1_row_id;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                }
                else if (tmp == (g_req_per_query - 1)) {
//                    req->rtype = FIRST_HS;
                    row_id = table_size - 3;

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < READ_HOTSPOT_RATIO){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else {
                #elif POS_HS == RANDOM
                    if (tmp == hotspot_position) {
                    // insert hotpost at the beginning
                    row_id = hs1_row_id;
//                        row_id =   vec_hot[0];

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < random_read_ratio){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else if (tmp == hotspot_position1){
                    // insert hotpost at the beginning
                    row_id = hs2_row_id;
//                        row_id =   vec_hot[1];

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < random_read_ratio){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else if (tmp == hotspot_position2){
                        // insert hotpost at the beginning
                        row_id = hs3_row_id;
//                        row_id =   vec_hot[2];

                        // 2-28 Support variable operation type for hotspot
                        double temp_r;
                        drand48_r(&_query_thd->buffer, &temp_r);
                        if(temp_r < random_read_ratio){
                            req->rtype = RD;
                        } else{
                            req->rtype = WR;
                        }

                        // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                        if(is_long && g_long_txn_read_ratio == 1){
                            req->rtype = RD;
                        }
                    } else {
                #endif
            #elif NUM_HS == 4
                #if  POS_HS == RANDOM
                    if (tmp == hotspot_position) {
                    // insert hotpost at the beginning
                    row_id = hs1_row_id;
//                        row_id =   vec_hot[0];

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < random_read_ratio){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else if (tmp == hotspot_position1){
                    // insert hotpost at the beginning
                    row_id = hs2_row_id;
//                        row_id =   vec_hot[1];

                    // 2-28 Support variable operation type for hotspot
                    double temp_r;
                    drand48_r(&_query_thd->buffer, &temp_r);
                    if(temp_r < random_read_ratio){
                        req->rtype = RD;
                    } else{
                        req->rtype = WR;
                    }

                    // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                    if(is_long && g_long_txn_read_ratio == 1){
                        req->rtype = RD;
                    }
                } else if (tmp == hotspot_position2){
                        // insert hotpost at the beginning
                        row_id = hs3_row_id;
//                        row_id =   vec_hot[2];

                        // 2-28 Support variable operation type for hotspot
                        double temp_r;
                        drand48_r(&_query_thd->buffer, &temp_r);
                        if(temp_r < random_read_ratio){
                            req->rtype = RD;
                        } else{
                            req->rtype = WR;
                        }

                        // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                        if(is_long && g_long_txn_read_ratio == 1){
                            req->rtype = RD;
                        }
                    } else if (tmp == hotspot_position3){
                        // insert hotpost at the beginning
                        row_id = hs4_row_id;
//                        row_id =   vec_hot[2];

                        // 2-28 Support variable operation type for hotspot
                        double temp_r;
                        drand48_r(&_query_thd->buffer, &temp_r);
                        if(temp_r < random_read_ratio){
                            req->rtype = RD;
                        } else{
                            req->rtype = WR;
                        }

                        // 2-26: Support read_only long transaction in synthetic YCSB. Reset its request type.
                        if(is_long && g_long_txn_read_ratio == 1){
                            req->rtype = RD;
                        }
                    }else {
                #endif
            #endif
#endif

        /*
         * 1. Determine the type of each request.
         */
        double r;
        // get a random number r to determine read/write ratio
        drand48_r(&_query_thd->buffer, &r);
        if (r < local_read_perc) {
            req->rtype = RD;
        } else if (r >= local_read_perc && r <= g_write_perc + local_read_perc) {
            req->rtype = WR;
        } else {
            req->rtype = SCAN;
            req->scan_len = SCAN_LEN;
        }

        /*
         * 2.1 Determine the row_id of the tuple that current request will access.
         */
        //uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
        //uint64_t row_id = zipf(table_size - 1, g_zipf_theta);
        row_id = get_new_row();

#if SYNTHETIC_YCSB
        }
#endif

        /*
         * 2.2 Finally determine the primary key value of the tuple that current request will access.
         */
        uint64_t primary_key = row_id * g_virtual_part_cnt + part_id;
        req->key = primary_key;
        assert(req->key < (g_synth_table_size / g_virtual_part_cnt));
        /*
        if (req->key >= (g_synth_table_size / g_virtual_part_cnt)) {
            printf("table size: %lu\n", g_synth_table_size / g_virtual_part_cnt);
            printf("WRONG KEY: %lu, req->key: %lu, rowid=%lu\n", primary_key, req->key, row_id);
            assert(false);
        }
        */

        /*
         * 3. Determine the value of each request.
         */
        int64_t rint64;
        lrand48_r(&_query_thd->buffer, &rint64);
        req->value = rint64 % (1<<8);


        // Make sure a single row is not accessed twice
        if (req->rtype == RD || req->rtype == WR) {
            if (all_keys.find(req->key) == all_keys.end()) {
                all_keys.insert(req->key);
                access_cnt ++;
            } else {
                tmp--;
                continue;
            }
        } else {
            bool conflict = false;
            for (UInt32 i = 0; i < req->scan_len; i++) {
                primary_key = (row_id + i) * g_part_cnt + part_id;
                if (all_keys.find( primary_key )
                    != all_keys.end())
                    conflict = true;
            }
            if (conflict) {
                tmp--;
                continue;
            }
            else {
                for (UInt32 i = 0; i < req->scan_len; i++)
                    all_keys.insert( (row_id + i) * g_part_cnt + part_id);
                access_cnt += SCAN_LEN;
            }
        }
        rid ++;
    }
    request_cnt = rid;
    assert(request_cnt == local_req_per_query);


    /*
     * Sort the requests according to request.key if a_key_order = true.
     * Actually g_key_order = false.
     */
    if (g_key_order) {
        // Sort the requests in key order.
        int a;
        int b;

#if SYNTHETIC_YCSB && (NUM_HS > 0)
        // works only for g_virtual_part_cnt = 1
	  uint64_t upper = (table_size - NUM_HS - 1) * g_virtual_part_cnt;
#endif
        for (int i = request_cnt - 1; i > 0; i--) {
            for (int j = 0; j < i; j ++) {
                a = j;
                b = j+1;

#if SYNTHETIC_YCSB && (NUM_HS > 0)
                if (requests[j].key > upper) {
					if (j != 0)
						a = j - 1;
					else
						continue;
				}
				if (requests[j+1].key > upper) {
					if (j + 1 != i)
						b = j + 1;
					else
						continue;
				}
#endif

                if (requests[a].key > requests[b].key) {
                    ycsb_request tmp_req = requests[a];
                    requests[a] = requests[b];
                    requests[b] = tmp_req;
                }
            }
        }

        part_num = 0;
        for (UInt32 i = 0; i < request_cnt - 1; i++) {

#if SYNTHETIC_YCSB && (NUM_HS > 0)
            if (requests[i].key > upper || (requests[i + 1].key > upper))
				continue;
#endif

            assert(requests[i].key < requests[i + 1].key);
//            printf("thread-%lu request[%d] %lu\n", thd_id, i, requests[i].key);
        }

//        printf("thread-%lu  \n", thd_id );
    }

}



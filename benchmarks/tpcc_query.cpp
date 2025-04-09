#include "query.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "tpcc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"

/**
 * Generate a tpcc_query.
 * @param thd_id
 * @param h_wl
 */
void tpcc_query::init(uint64_t thd_id, workload * h_wl) {
    double x = (double)(rand() % 100) / 100.0;
    part_to_access = (uint64_t *)
            mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);

#if TPCC_NP == false
    if (x < 0.04)
        gen_stock_level(thd_id);
    else if (x < 0.04 + 0.04)
        gen_delivery(thd_id);
    else if (x < 0.04 + 0.04 + 0.04)
        gen_order_status(thd_id);
    else if (x < 0.04 + 0.04 + 0.04 + 0.43)
        gen_payment(thd_id);
    else
        gen_new_order(thd_id);
#else
    if (x < g_perc_payment)
  	gen_payment(thd_id);
  else
  	gen_new_order(thd_id);
#endif
}


void tpcc_query::gen_query2(uint64_t thd_id) {
//    type = TPCC_QUERY2;
}
/**
 * Initialize a Payment query.
 * @param thd_id
 */
void tpcc_query::gen_payment(uint64_t thd_id) {
    type = TPCC_PAYMENT;
    tpcc_query_payment& arg = args.payment;

//    read_committed = false;
//    readonly = false;
////    request_cnt = 4;

    if (FIRST_PART_LOCAL)
        arg.w_id = thd_id % g_num_wh + 1;
    else
        arg.w_id = URand(1, g_num_wh, thd_id % g_num_wh);

//    d_w_id = arg.w_id;
    uint64_t part_id = wh_to_part(arg.w_id);
    part_to_access[0] = part_id;
    part_num = 1;

    arg.d_id = URand(1, DIST_PER_WARE, arg.w_id-1);
    arg.h_amount = URand(1, 5000, arg.w_id-1);
    int x = URand(1, 100, arg.w_id-1);            // access home/remote warehouse
    int y = URand(1, 100, arg.w_id-1);            // select customer by last_name/cust_id


    if(x <= 85) {
        // home warehouse
        arg.c_d_id = arg.d_id;
        arg.c_w_id = arg.w_id;
    } else {
        // remote warehouse
        arg.c_d_id = URand(1, DIST_PER_WARE, arg.w_id-1);
        if(g_num_wh > 1) {
            while((arg.c_w_id = URand(1, g_num_wh, arg.w_id-1)) == arg.w_id) {}
            if (wh_to_part(arg.w_id) != wh_to_part(arg.c_w_id)) {
                part_to_access[1] = wh_to_part(arg.c_w_id);
                part_num = 2;
            }
        } else
            arg.c_w_id = arg.w_id;
    }
    if(y <= 60) {
        // by last name
        arg.by_last_name = true;
        Lastname(NURand(255,0,999,arg.w_id-1),arg.c_last);
    } else {
        // by cust id
        arg.by_last_name = false;
        arg.c_id = NURand(1023, 1, g_cust_per_dist,arg.w_id-1);
    }
}

/**
 * Initialize a New_Order query.
 * @param thd_id
 */
void tpcc_query::gen_new_order(uint64_t thd_id) {
    type = TPCC_NEW_ORDER;
    tpcc_query_new_order& arg = args.new_order;

//    read_committed = false;
//    readonly = false;

    if (FIRST_PART_LOCAL)
        arg.w_id = thd_id % g_num_wh + 1;
    else
        arg.w_id = URand(1, g_num_wh, thd_id % g_num_wh);

    arg.d_id = URand(1, DIST_PER_WARE, arg.w_id-1);
    arg.c_id = NURand(1023, 1, g_cust_per_dist, arg.w_id-1);
//    rbk = URand(1, 100, arg.w_id-1);
    arg.ol_cnt = URand(5, 15, arg.w_id-1);
//    ol_cnt = URand(OL_CNT_ST, OL_CNT_ED, w_id-1);
    arg.o_entry_d = 2013;
    arg.items = (Item_no *) _mm_malloc(sizeof(Item_no) * arg.ol_cnt, 64);
//    arg.remote = false;
    part_to_access[0] = wh_to_part(arg.w_id);
    part_num = 1;

    for (UInt32 oid = 0; oid < arg.ol_cnt; oid ++) {
        arg.items[oid].ol_i_id = NURand(8191, 1, g_max_items, arg.w_id-1);
#if TPCC_USER_ABORT
        // XXX(zhihan): 1% of the New-Order transactions are chosen at random to
        // simulate user data entry errors and exercise the performance of
        // rolling back update transactions.
        // If this is the last item on the order and rbk = 1 (chosen from [1,
        // 100]), then the item number is set to an unused value.
        if ((oid == ol_cnt - 1) && (rbk == 1)) {
            items[oid].ol_i_id = 0;
        }
#endif
        UInt32 x = URand(1, 100, arg.w_id-1);
        if (x > 1 || g_num_wh == 1)
            arg.items[oid].ol_supply_w_id = arg.w_id;
        else  {
            while((arg.items[oid].ol_supply_w_id = URand(1, g_num_wh, arg.w_id-1)) == arg.w_id) {}
//            remote = true;
        }
        arg.items[oid].ol_quantity = URand(1, 10, arg.w_id-1);
    }
    // Remove duplicate items
    for (UInt32 i = 0; i < arg.ol_cnt; i ++) {
        for (UInt32 j = 0; j < i; j++) {
            if (arg.items[i].ol_i_id == arg.items[j].ol_i_id) {
                for (UInt32 k = i; k < arg.ol_cnt - 1; k++)
                    arg.items[k] = arg.items[k + 1];
                arg.ol_cnt --;
                i--;
            }
        }
    }
    for (UInt32 i = 0; i < arg.ol_cnt; i ++)
        for (UInt32 j = 0; j < i; j++)
            assert(arg.items[i].ol_i_id != arg.items[j].ol_i_id);
    // update part_to_access
    for (UInt32 i = 0; i < arg.ol_cnt; i ++) {
        UInt32 j;
        for (j = 0; j < part_num; j++ )
            if (part_to_access[j] == wh_to_part(arg.items[i].ol_supply_w_id))
                break;
        if (j == part_num) // not found! add to it.
            part_to_access[part_num ++] = wh_to_part( arg.items[i].ol_supply_w_id );
    }
}

void
tpcc_query::gen_order_status(uint64_t thd_id) {
    type = TPCC_ORDER_STATUS;
    readonly = true;
    read_committed = false;
//    request_cnt = 3;
    tpcc_query_order_status& arg = args.order_status;

    if (FIRST_PART_LOCAL)
        arg.w_id = thd_id % g_num_wh + 1;
    else
        arg.w_id = URand(1, g_num_wh, thd_id % g_num_wh);

    arg.d_id = URand(1, DIST_PER_WARE, thd_id);

    int y = URand(1, 100, thd_id);
    if (y <= 60) {
        // by last name
        arg.by_last_name = true;
        Lastname(NURand(255, 0, 999, thd_id), arg.c_last);
    } else {
        // by cust id
        arg.by_last_name = false;
        arg.c_id = NURand(1023, 1, g_cust_per_dist, thd_id);
    }
}

void tpcc_query::gen_delivery(uint64_t thd_id) {
    type = TPCC_DELIVERY;
    tpcc_query_delivery& arg = args.delivery;
//    request_cnt = 60;
    read_committed = false;
    readonly = false;

    if (FIRST_PART_LOCAL)
        arg.w_id = thd_id % g_num_wh + 1;
    else
        arg.w_id = URand(1, g_num_wh, thd_id % g_num_wh);

    arg.o_carrier_id = URand(1, DIST_PER_WARE, thd_id);
    arg.ol_delivery_d = 2013;
}

void tpcc_query::gen_stock_level(uint64_t thd_id) {
    type = TPCC_STOCK_LEVEL;
    readonly = true;
    read_committed = false;
//    request_cnt = 2;
    tpcc_query_stock_level& arg = args.stock_level;

    if (FIRST_PART_LOCAL)
        arg.w_id = thd_id % g_num_wh + 1;
    else
        arg.w_id = URand(1, g_num_wh, thd_id % g_num_wh);

    arg.d_id = URand(1, DIST_PER_WARE, thd_id);
    arg.threshold = URand(10, 20, thd_id);
}

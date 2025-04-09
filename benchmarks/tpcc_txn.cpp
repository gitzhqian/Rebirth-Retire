#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"


#define RETIRE_ROW(row_cnt) { \
  access_cnt = row_cnt - 1; \
  if (retire_row(access_cnt) == Abort) \
    return finish(Abort); \
}

void tpcc_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
    txn_man::init(h_thd, h_wl, thd_id);
    _wl = (tpcc_wl *) h_wl;
}

RC tpcc_txn_man::run_txn(base_query * query) {
    tpcc_query * m_query = (tpcc_query *) query;
#if CC_ALG == IC3
    curr_type = m_query->type;
    curr_piece = 0;
#endif
    switch (m_query->type) {
        case TPCC_PAYMENT :
            return run_payment(m_query); break;
        case TPCC_NEW_ORDER :
            return run_new_order(m_query); break;
//            return RCOK; break;
        case TPCC_ORDER_STATUS :
            return run_order_status(m_query); break;
//            return RCOK; break;
        case TPCC_DELIVERY :
//            return run_delivery(m_query); break;
            return RCOK; break;
        case TPCC_STOCK_LEVEL :
            return run_stock_level(m_query); break;
//            return RCOK; break;
        default:
            assert(false); return Abort;
    }
}

/**
 * 1. Update WAREHOUSE, DISTRICT, CUSTOMER table.
 * 2. Insert a tuple into HISTORY table.
 * @param query
 * @return
 */
RC tpcc_txn_man::run_payment(tpcc_query * query) {

    auto& arg = query->args.payment;

#if CC_ALG == BAMBOO && (THREAD_CNT > 1)
    int access_cnt;
#endif
    // declare all variables
    RC rc = RCOK;
    uint64_t key;
    itemid_t * item;
    int cnt;
    uint64_t row_id;
    // rows
    row_t * r_wh;
    row_t * r_wh_local;
    row_t * r_cust;
    row_t * r_cust_local;
    row_t * r_hist;
    // values
#if !COMMUTATIVE_OPS
    double tmp_value;
#endif
    char w_name[11];
    char * tmp_str;
    char d_name[11];
    double c_balance;
    double c_ytd_payment;
    double c_payment_cnt;
    char * c_credit;

    uint64_t w_id = arg.w_id;
    uint64_t c_w_id = arg.c_w_id;
    /*====================================================+
        EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
        WHERE w_id=:w_id;
    +====================================================*/
    /*===================================================================+
        EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
        INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
        FROM warehouse WHERE w_id=:w_id;
    +===================================================================*/

    // TODO: for variable length variable (string). Should store the size of
    //  the variable.
    //BEGIN: [WAREHOUSE] RW
#if CC_ALG == IC3
    warehouse_piece:
    begin_piece(0);
#endif
    //1. update warehouse
    key = arg.w_id;
//    INDEX * index = _wl->i_warehouse;
//    item = index_read(index, key, wh_to_part(w_id));
    auto index_warehouse = _wl->i_warehouse;
    auto part_id = wh_to_part(w_id);
    r_wh_local = search(index_warehouse, key, part_id,  g_wh_update ? WR : RD);
//    assert(item != NULL);
//    r_wh = ((row_t *)item->location);
//#if !COMMUTATIVE_OPS
//    r_wh_local = get_row(r_wh, WR);
//#else
//    r_wh_local = get_row(r_wh, RD);
//#endif
    if (r_wh_local == NULL) {
        return finish(Abort);
    }

#if !COMMUTATIVE_OPS
    //update the balance to the warehouse
    r_wh_local->get_value(W_YTD, tmp_value);
    if (g_wh_update) {
        r_wh_local->set_value(W_YTD, tmp_value + arg.h_amount);
    }
#else
    inc_value(W_YTD, query->h_amount); // will increment at commit time
#endif
    // bamboo: retire lock for wh
#if (CC_ALG == BAMBOO) && (THREAD_CNT > 1) && !COMMUTATIVE_OPS
    RETIRE_ROW(row_cnt)
#endif
#if CC_ALG == REBIRTH_RETIRE
    #if PASSIVE_RETIRE
           accesses[row_cnt-1]->lock_entry->has_write = true;
    #else
         if (retire_row(row_cnt-1) == Abort) {
            return finish(Abort);
         }
    #endif
#endif
    //get a copy of warehouse name
    tmp_str = r_wh_local->get_value(W_NAME);
    memcpy(w_name, tmp_str, 10);
    w_name[10] = '\0';
#if CC_ALG == IC3
    if (end_piece(0) != RCOK)
        goto warehouse_piece;
#endif

#if CC_ALG == IC3
    district_piece:
    begin_piece(1);
#endif
    /*====================================================================+
      EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
      INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
      FROM district
      WHERE d_w_id=:w_id AND d_id=:d_id;
    +====================================================================*/
    /*=====================================================+
        EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
        WHERE d_w_id=:w_id AND d_id=:d_id;
    +=====================================================*/
    //2. update district
    key = distKey(arg.d_id, arg.w_id);
//    item = index_read(_wl->i_district, key, wh_to_part(w_id));
    auto r_dist_local = search(_wl->i_district, key, part_id, WR);
    if (r_dist_local == NULL) {
        return finish(Abort);
    }

#if !COMMUTATIVE_OPS
    r_dist_local->get_value(D_YTD, tmp_value);
    r_dist_local->set_value(D_YTD, tmp_value + arg.h_amount);
#else
    inc_value(D_YTD, query->h_amount); // will increment at commit time
#endif

#if (CC_ALG == BAMBOO) && (THREAD_CNT > 1) && !COMMUTATIVE_OPS
    RETIRE_ROW(row_cnt)
#endif
#if CC_ALG == REBIRTH_RETIRE
    #if PASSIVE_RETIRE
           accesses[row_cnt-1]->lock_entry->has_write = true;
    #else
         if (retire_row(row_cnt-1) == Abort) {
            return finish(Abort);
         }
    #endif
#endif

    tmp_str = r_dist_local->get_value(D_NAME);
    memcpy(d_name, tmp_str, 10);
    d_name[10] = '\0';

#if CC_ALG == IC3
    if(end_piece(1) != RCOK)
        goto district_piece;

    customer_piece:
    begin_piece(2);
#endif

    //3. update customer
    if (arg.by_last_name) {
        //3. update customer
        uint64_t key = custNPKey(arg.c_last, arg.c_d_id, arg.c_w_id);
        auto index = _wl->i_customer_last;
        itemid_t* items[100];
        size_t count = 100;
        auto rc = index_read_multiple(index, key, items, count, part_id);
        if (rc != RCOK || count == 0) {
            return finish(Abort);
        }
        auto mid = items[count / 2];
        r_cust = ((row_t *)mid->location);
        r_cust_local = get_row(r_cust, WR);
    }
    else { // search customers by cust_id
        key = custKey(arg.c_id, arg.c_d_id, arg.c_w_id);
        auto index = _wl->i_customer_id;
        r_cust_local = search(index, key, part_id, WR);
    }
    /*======================================================================+
         EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
         WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
     +======================================================================*/
//    r_cust_local = get_row(r_cust, WR);
    if (r_cust_local == NULL) {
        return finish(Abort);
    }
    r_cust_local->get_value(C_BALANCE, c_balance);
    r_cust_local->set_value(C_BALANCE, c_balance - arg.h_amount);
    r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
    r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + arg.h_amount);
    r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
    r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

    c_credit = r_cust_local->get_value(C_CREDIT);
    if ( strstr(c_credit, "BC") && !TPCC_SMALL ) {
        /*=====================================================+
            EXEC SQL SELECT c_data
            INTO :c_data
            FROM customer
            WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
        +=====================================================*/
        char c_new_data[501];
        r_cust_local->set_value("C_DATA", c_new_data);
    }

#if (CC_ALG == BAMBOO) && (THREAD_CNT > 1)
    RETIRE_ROW(row_cnt)
#endif
#if CC_ALG == REBIRTH_RETIRE
    #if PASSIVE_RETIRE
           accesses[row_cnt-1]->lock_entry->has_write = true;
    #else
         if (retire_row(row_cnt-1) == Abort) {
            return finish(Abort);
         }
    #endif
#endif

#if CC_ALG == IC3
    if(end_piece(2) != RCOK)
        goto customer_piece;
#endif

    //update h_data according to spec
    char h_data[25];
    strncpy(h_data, w_name, 10);
    int length = strlen(h_data);
    if (length > 10) length = 10;
    strcpy(&h_data[length], "    ");
    strncpy(&h_data[length + 4], d_name, 10);
    h_data[length+14] = '\0';
    /*=============================================================================+
      EXEC SQL INSERT INTO
      history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
      VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
      +=============================================================================*/
    //not causing the buffer overflow
    _wl->t_history->get_new_row(r_hist, 0, row_id);
    r_hist->set_value(H_C_ID, arg.c_id);
    r_hist->set_value(H_C_D_ID, arg.c_d_id);
    r_hist->set_value(H_C_W_ID, c_w_id);
    r_hist->set_value(H_D_ID, arg.d_id);
    r_hist->set_value(H_W_ID, w_id);
    int64_t date = 2013;
    r_hist->set_value(H_DATE, date);
    r_hist->set_value(H_AMOUNT, arg.h_amount);
#if !TPCC_SMALL
    r_hist->set_value(H_DATA, h_data);
#endif

    assert( rc == RCOK );
    return finish(rc);
}


/**
 * 1. Read WAREHOUSE, DISTRICT, CUSTOMER table.
 * 2. Insert a tuple in NEW_ORDER, Order table.
 * 3. Read ol_cnt tuples in ITEM, STOCK table.
 * @param query
 * @return
 */
RC tpcc_txn_man::run_new_order(tpcc_query * query) {
    auto& arg = query->args.new_order;

    RC rc = RCOK;
    uint64_t key;
    itemid_t * item;
//    bool remote = arg.remote;
    uint64_t w_id = arg.w_id;
    uint64_t d_id = arg.d_id;
    uint64_t c_id = arg.c_id;
    uint64_t ol_cnt = arg.ol_cnt;
    uint64_t part_id = wh_to_part(w_id);

    // declare vars
    double w_tax;
    row_t * r_cust;
    row_t * r_cust_local;
    row_t * r_dist;
    row_t * r_dist_local;
    // d_tax: used only when implementing full tpcc
    double d_tax;
//  int64_t o_id;
    int64_t o_d_id;
    uint64_t row_id;
    row_t * r_order;
    row_t * r_order_cust;
    int64_t all_local;
    row_t * r_no;
    // order
    int sum=0;
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t ol_quantity;
    row_t * r_item;
    row_t * r_item_local;
    row_t * r_stock;
    row_t * r_stock_local;
    int64_t i_price;
    uint64_t stock_key;
    UInt64 s_quantity;
    int64_t s_remote_cnt;
    int64_t s_ytd;
    int64_t s_order_cnt;
    uint64_t quantity;
    int64_t ol_amount;
    row_t * r_ol;

    char* s_dist_01;
    char* s_dist_02;
    char* s_dist_03;
    char* s_dist_04;
    char* s_dist_05;
    char* s_dist_06;
    char* s_dist_07;
    char* s_dist_08;
    char* s_dist_09;
    char* s_dist_10;
#if IC3_MODIFIED_TPCC
    double tmp_value;
#endif

    /*=======================================================================+
    EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
        INTO :c_discount, :c_last, :c_credit, :w_tax
        FROM customer, warehouse
        WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
    +========================================================================*/

#if CC_ALG == IC3
    warehouse_piece: // 0
    begin_piece(0);
#endif
    // 1. search warehouse
    key = w_id;
//    index = _wl->i_warehouse;
//    item = index_read(index, key, wh_to_part(w_id));
//    assert(item != NULL);
//    row_t * r_wh = ((row_t *)item->location);
//    row_t * r_wh_local = get_row(r_wh, RD);
    row_t * r_wh_local = search(_wl->i_warehouse, key, part_id, RD);
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    //retrieve the tax of warehouse
    r_wh_local->get_value(W_TAX, w_tax);

#if IC3_MODIFIED_TPCC
    r_wh_local->get_value(W_YTD, tmp_value);
#endif
#if CC_ALG == IC3
    if (end_piece(0) != RCOK)
        goto warehouse_piece;

    district_piece:
    begin_piece(1);
#endif
    /*==================================================+
    EXEC SQL SELECT d_next_o_id, d_tax
        INTO :d_next_o_id, :d_tax
        FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
    EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
        WH ERE d _id = :d_id AN D d _w _id = :w _id ;
    +===================================================*/
    //2. update district
    key = distKey(d_id, w_id);
//    item = index_read(_wl->i_district, key, wh_to_part(w_id));
//    assert(item != NULL);
//    r_dist = ((row_t *)item->location);
//    r_dist_local = get_row(r_dist, WR);
    r_dist_local = search(_wl->i_district, key, part_id, WR);
    if (r_dist_local == NULL) {
        return finish(Abort);
    }

    //d_tax = *(double *) r_dist_local->get_value(D_TAX);
    r_dist_local->get_value(D_TAX, d_tax);
    int64_t o_id;
    r_dist_local->get_value(D_NEXT_O_ID, o_id);
//  o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);

    o_id ++;
    r_dist_local->set_value(D_NEXT_O_ID, o_id);

#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
    if (retire_row(row_cnt-1) == Abort){
        return finish(Abort);
    }
#endif
#if CC_ALG == REBIRTH_RETIRE
    #if PASSIVE_RETIRE
           accesses[row_cnt-1]->lock_entry->has_write = true;
    #else
         if (retire_row(row_cnt-1) == Abort) {
            return finish(Abort);
         }
    #endif
#endif

#if CC_ALG == IC3
    if (end_piece(1) != RCOK)
        goto district_piece;

    customer_piece:
    begin_piece(2);
#endif
    //3. search customer
    key = custKey(c_id, d_id, w_id);
//    index = _wl->i_customer_id;
//    item = index_read(index, key, wh_to_part(w_id));
//    assert(item != NULL);
//    r_cust = (row_t *) item->location;
//    r_cust_local = get_row(r_cust, RD);
    r_cust_local =search(_wl->i_customer_id, key, part_id, RD);
    if (r_cust_local == NULL) {
        return finish(Abort);
    }
    //retrieve data
    uint64_t c_discount;
    if(!TPCC_SMALL) {
        r_cust_local->get_value(C_LAST);
        r_cust_local->get_value(C_CREDIT);
    }
    r_cust_local->get_value(C_DISCOUNT, c_discount);

#if CC_ALG == IC3
    if (end_piece(2) != RCOK)
        goto customer_piece;

    neworder_piece: // 3
    begin_piece(3);
#endif
    /*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
//  /*
    //4. insert neworder
    _wl->t_neworder->get_new_row(r_no, 0, row_id);
    uint64_t neword_key = neworderKey(o_id, d_id, w_id);
    r_no->set_primary_key(neword_key);
    r_no->set_value(NO_O_ID, o_id);
    r_no->set_value(NO_D_ID, d_id);
    r_no->set_value(NO_W_ID, w_id);
    insert_row(r_no, _wl->t_neworder);
    insert_idx(_wl->i_neworder, key, r_no, part_id);
// */
#if CC_ALG == IC3
    if (end_piece(3) != RCOK)
        goto neworder_piece;

    order_piece: // 4
    begin_piece(4);
#endif
    /*========================================================================================+
    EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
        VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
    +========================================================================================*/
// /*
    //5. insert order
    _wl->t_order->get_new_row(r_order, 0, row_id);
    uint64_t ord_key = orderKey(o_id, d_id, w_id);
    r_order->set_primary_key(ord_key);
    r_order->set_value(O_ID, o_id);
    r_order->set_value(O_C_ID, c_id);
    r_order->set_value(O_D_ID, d_id);
    r_order->set_value(O_W_ID, w_id);
    r_order->set_value(O_ENTRY_D, arg.o_entry_d);
    r_order->set_value(O_OL_CNT, ol_cnt);
//  o_d_id=*(int64_t *) r_order->get_value(O_D_ID);
//  o_d_id=d_id;
//  all_local = (remote? 0 : 1);
//  r_order->set_value(O_ALL_LOCAL, all_local);
    insert_row(r_order, _wl->t_order);
    _wl->t_order->get_new_row(r_order_cust, 0, row_id);
    insert_row(r_order_cust, _wl->t_order);
    r_order_cust->set_primary_key(orderCustKey(o_id, c_id, d_id, w_id));
    memcpy(r_order_cust->get_data(), r_order_cust->get_data(), r_order_cust->get_tuple_size());
      {
        insert_idx(_wl->i_order, orderKey(o_id, d_id, w_id), r_order, part_id);
      }
      {
        insert_idx(_wl->i_order_cust, orderCustKey(o_id, c_id, d_id, w_id), r_order_cust, part_id);
      }
    //may need to set o_ol_cnt=ol_cnt;
//  */
    //o_d_id=d_id;
#if CC_ALG == IC3
    if (end_piece(4) != RCOK)
        goto order_piece;

    item_piece: // 5
    begin_piece(5);
    /*===========================================+
    EXEC SQL SELECT i_price, i_name , i_data
        INTO :i_price, :i_name, :i_data
        FROM item
        WHERE i_id = :ol_i_id;
    +===========================================*/
    for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {
        ol_i_id = query->items[ol_number].ol_i_id;
#if TPCC_USER_ABORT
        // XXX(zhihan): if key is invalid, abort. user-initiated abort
    // according to tpc-c documentation
    if (ol_i_id == 0)
      return finish(ERROR);
#endif
        ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
        ol_quantity = query->items[ol_number].ol_quantity;
        key = ol_i_id;
        item = index_read(_wl->i_item, key, 0);
        assert(item != NULL);
        r_item = ((row_t *)item->location);
        r_item_local = get_row(r_item, RD);
        if (r_item_local == NULL) {
            return finish(Abort);
        }
        r_item_local->get_value(I_PRICE, i_price);
        r_item_local->get_value(I_NAME);
        r_item_local->get_value(I_DATA);
        assert(r_item_local->data);
    }

    if (end_piece(5) != RCOK)
        goto item_piece;


    stock_piece: // 6
    begin_piece(6);
    /*===================================================================+
    EXEC SQL SELECT s_quantity, s_data,
            s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
            s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
        INTO :s_quantity, :s_data,
            :s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
            :s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
        FROM stock
        WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
    EXEC SQL UPDATE stock SET s_quantity = :s_quantity
        WHERE s_i_id = :ol_i_id
        AND s_w_id = :ol_supply_w_id;
    +===============================================*/
    for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {
        ol_i_id = query->items[ol_number].ol_i_id;
        ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
        ol_quantity = query->items[ol_number].ol_quantity;
        stock_key = stockKey(ol_i_id, ol_supply_w_id);
        stock_index = _wl->i_stock;
        index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
        assert(stock_item != NULL);
        r_stock = ((row_t *)stock_item->location);
        r_stock_local = get_row(r_stock, WR);
        assert(r_stock_local->data);
        if (r_stock_local == NULL) {
            return finish(Abort);
        }
        // XXX s_dist_xx are not retrieved.
        s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
        //try to retrieve s_dist_xx
#if !TPCC_SMALL
        /*
        s_dist_01=(char *)r_stock_local->get_value(S_DIST_01);
        s_dist_02=(char *)r_stock_local->get_value(S_DIST_02);
        s_dist_03=(char *)r_stock_local->get_value(S_DIST_03);
        s_dist_04=(char *)r_stock_local->get_value(S_DIST_04);
        s_dist_05=(char *)r_stock_local->get_value(S_DIST_05);
        s_dist_06=(char *)r_stock_local->get_value(S_DIST_06);
        s_dist_07=(char *)r_stock_local->get_value(S_DIST_07);
        s_dist_08=(char *)r_stock_local->get_value(S_DIST_08);
        s_dist_09=(char *)r_stock_local->get_value(S_DIST_09);
        s_dist_10=(char *)r_stock_local->get_value(S_DIST_10);
        //char * s_data = "test";
        */
        r_stock_local->get_value(S_YTD, s_ytd);
        r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
        r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
        r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
        //s_data = r_stock_local->get_value(S_DATA);
#endif
        if (remote) {
            s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
            s_remote_cnt ++;
            r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
        }
        if (s_quantity > ol_quantity + 10) {
            quantity = s_quantity - ol_quantity;
        } else {
            quantity = s_quantity - ol_quantity + 91;
        }
        r_stock_local->set_value(S_QUANTITY, &quantity);
    }
    if (end_piece(6) != RCOK)
        goto stock_piece;

    orderline_piece: // 7
    begin_piece(7);
    /*====================================================+
    EXEC SQL INSERT
        INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
            ol_i_id, ol_supply_w_id,
            ol_quantity, ol_amount, ol_dist_info)
        VALUES(:o_id, :d_id, :w_id, :ol_number,
            :ol_i_id, :ol_supply_w_id,
            :ol_quantity, :ol_amount, :ol_dist_info);
    +====================================================*/
    /*
    for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {
      ol_i_id = query->items[ol_number].ol_i_id;
      ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
      ol_quantity = query->items[ol_number].ol_quantity;
      // XXX district info is not inserted.
      _wl->t_orderline->get_new_row(r_ol, 0, row_id);
      r_ol->set_value(OL_O_ID, &o_id);
      r_ol->set_value(OL_D_ID, &d_id);
      r_ol->set_value(OL_W_ID, &w_id);
      r_ol->set_value(OL_NUMBER, &ol_number);
      r_ol->set_value(OL_I_ID, &ol_i_id);
      //deal with district
  #if !TPCC_SMALL
      if(o_d_id==1){
        r_ol->set_value(OL_DIST_INFO, &s_dist_01);
      }else if(o_d_id==2){
        r_ol->set_value(OL_DIST_INFO, &s_dist_02);
      }else if(o_d_id==3){
        r_ol->set_value(OL_DIST_INFO, &s_dist_03);
      }else if(o_d_id==4){
        r_ol->set_value(OL_DIST_INFO, &s_dist_04);
      }else if(o_d_id==5){
        r_ol->set_value(OL_DIST_INFO, &s_dist_05);
      }else if(o_d_id==6){
        r_ol->set_value(OL_DIST_INFO, &s_dist_06);
      }else if(o_d_id==7){
        r_ol->set_value(OL_DIST_INFO, &s_dist_07);
      }else if(o_d_id==8){
        r_ol->set_value(OL_DIST_INFO, &s_dist_08);
      }else if(o_d_id==9){
        r_ol->set_value(OL_DIST_INFO, &s_dist_09);
      }else if(o_d_id==10){
        r_ol->set_value(OL_DIST_INFO, &s_dist_10);
      }
  #endif
  #if !TPCC_SMALL
      ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
      r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
      r_ol->set_value(OL_QUANTITY, &ol_quantity);
      r_ol->set_value(OL_AMOUNT, &ol_amount);
  #endif
  #if !TPCC_SMALL
      sum+=ol_amount;
  #endif
      //insert_row(r_ol, _wl->t_orderline);
    }
      */
    if (end_piece(7) != RCOK)
        goto orderline_piece;
#else // if CC_ALG != IC3

      for (uint64_t ol_number = 0; ol_number < ol_cnt; ol_number++) {
        ol_i_id = arg.items[ol_number].ol_i_id;
#if TPCC_USER_ABORT
        // XXX(zhihan): if key is invalid, abort. user-initiated abort
        // according to tpc-c documentation
        if (ol_i_id == 0)
          return finish(ERROR);
#endif
        ol_supply_w_id = arg.items[ol_number].ol_supply_w_id;
        ol_quantity = arg.items[ol_number].ol_quantity;
        /*===========================================+
        EXEC SQL SELECT i_price, i_name , i_data
            INTO :i_price, :i_name, :i_data
            FROM item
            WHERE i_id = :ol_i_id;
        +===========================================*/
        //6. search item
        key = ol_i_id;
//        item = index_read(_wl->i_item, key, 0);
//        assert(item != NULL);
//        r_item = ((row_t *)item->location);
//        r_item_local = get_row(r_item, RD);
        r_item_local = search(_wl->i_item, key, part_id, RD);
        if (r_item_local == NULL) {
            return finish(Abort);
        }
        r_item_local->get_value(I_PRICE, i_price);
        r_item_local->get_value(I_NAME);
        r_item_local->get_value(I_DATA);
        /*===================================================================+
        EXEC SQL SELECT s_quantity, s_data,
                s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
                s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
            INTO :s_quantity, :s_data,
                :s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
                :s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
            FROM stock
            WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
        EXEC SQL UPDATE stock SET s_quantity = :s_quantity
            WHERE s_i_id = :ol_i_id
            AND s_w_id = :ol_supply_w_id;
        +===============================================*/
        // 7. update stock
        stock_key = stockKey(ol_i_id, ol_supply_w_id);
//        stock_index = _wl->i_stock;
//        index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
//        assert(item != NULL);
//        r_stock = ((row_t *)stock_item->location);
//        r_stock_local = get_row(r_stock, WR);
        r_stock_local = search(_wl->i_stock, stock_key, part_id, WR);
        if (r_stock_local == NULL) {
            return finish(Abort);
        }

        // XXX s_dist_xx are not retrieved.
        s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
        //try to retrieve s_dist_xx
#if !TPCC_SMALL
         /*
        s_dist_01=(char *)r_stock_local->get_value(S_DIST_01);
        s_dist_02=(char *)r_stock_local->get_value(S_DIST_02);
        s_dist_03=(char *)r_stock_local->get_value(S_DIST_03);
        s_dist_04=(char *)r_stock_local->get_value(S_DIST_04);
        s_dist_05=(char *)r_stock_local->get_value(S_DIST_05);
        s_dist_06=(char *)r_stock_local->get_value(S_DIST_06);
        s_dist_07=(char *)r_stock_local->get_value(S_DIST_07);
        s_dist_08=(char *)r_stock_local->get_value(S_DIST_08);
        s_dist_09=(char *)r_stock_local->get_value(S_DIST_09);
        s_dist_10=(char *)r_stock_local->get_value(S_DIST_10);
        */
        //char * s_data = "test";
        r_stock_local->get_value(S_YTD, s_ytd);
        r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
        r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
        r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
        //s_data = r_stock_local->get_value(S_DATA);
#endif
        bool remote = ol_supply_w_id != arg.w_id;
        if (remote) {
            s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
            s_remote_cnt ++;
            r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
        }

        if (s_quantity > ol_quantity + 10) {
            quantity = s_quantity - ol_quantity;
        } else {
            quantity = s_quantity - ol_quantity + 91;
        }
        r_stock_local->set_value(S_QUANTITY, &quantity);

#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
        if (retire_row(row_cnt-1) == Abort){
            return finish(Abort);
        }
#endif
#if CC_ALG == REBIRTH_RETIRE
        #if PASSIVE_RETIRE
           accesses[row_cnt-1]->lock_entry->has_write = true;
        #else
         if (retire_row(row_cnt-1) == Abort) {
            return finish(Abort);
         }
        #endif
#endif

        /*====================================================+
        EXEC SQL INSERT
            INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
                ol_i_id, ol_supply_w_id,
                ol_quantity, ol_amount, ol_dist_info)
            VALUES(:o_id, :d_id, :w_id, :ol_number,
                :ol_i_id, :ol_supply_w_id,
                :ol_quantity, :ol_amount, :ol_dist_info);
        +====================================================*/
        // XXX district info is not inserted.
// 	/*
        // 8. insert orderline
        _wl->t_orderline->get_new_row(r_ol, 0, row_id);
        uint64_t ordline_key = orderlineKey(ol_number, o_id, d_id, w_id);
        r_ol->set_primary_key(ordline_key);
        r_ol->set_value(OL_O_ID, &o_id);
        r_ol->set_value(OL_D_ID, &d_id);
        r_ol->set_value(OL_W_ID, &w_id);
        r_ol->set_value(OL_NUMBER, &ol_number);
        r_ol->set_value(OL_I_ID, &ol_i_id);
        //deal with district
#if !TPCC_SMALL
        if(o_d_id==1){
            r_ol->set_value(OL_DIST_INFO, &s_dist_01);
        }else if(o_d_id==2){
            r_ol->set_value(OL_DIST_INFO, &s_dist_02);
        }else if(o_d_id==3){
            r_ol->set_value(OL_DIST_INFO, &s_dist_03);
        }else if(o_d_id==4){
            r_ol->set_value(OL_DIST_INFO, &s_dist_04);
        }else if(o_d_id==5){
            r_ol->set_value(OL_DIST_INFO, &s_dist_05);
        }else if(o_d_id==6){
            r_ol->set_value(OL_DIST_INFO, &s_dist_06);
        }else if(o_d_id==7){
            r_ol->set_value(OL_DIST_INFO, &s_dist_07);
        }else if(o_d_id==8){
            r_ol->set_value(OL_DIST_INFO, &s_dist_08);
        }else if(o_d_id==9){
            r_ol->set_value(OL_DIST_INFO, &s_dist_09);
        }else if(o_d_id==10){
            r_ol->set_value(OL_DIST_INFO, &s_dist_10);
        }
#endif
#if !TPCC_SMALL
        ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
        r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
        r_ol->set_value(OL_QUANTITY, &ol_quantity);
        r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif

#if !TPCC_SMALL
    sum+=ol_amount;
#endif
        insert_row(r_ol, _wl->t_orderline);
        insert_idx(_wl->i_orderline, ordline_key, r_ol, part_id);
// 	 */
    }

#endif // if CC_ALG == IC3

    assert( rc == RCOK );
    return finish(rc);
}

RC tpcc_txn_man::run_delivery(tpcc_query * query) {
    auto& arg = query->args.delivery;
    int64_t o_id = 0;

  for (uint64_t d_id = 1; d_id <= DIST_PER_WARE; d_id++)
  {
    o_id = 0;
    if (!delivery_getNewOrder_deleteNewOrder(d_id, arg.w_id, o_id)) {
      return finish(Abort);
    }
    //1.update neworder
#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
      if (retire_row(row_cnt-1) == Abort){
          return finish(Abort);
      }
#endif
    #if CC_ALG == REBIRTH_RETIRE && PASSIVE_RETIRE
      accesses[row_cnt-1]->lock_entry->has_write = true;
    #endif

    // No new order for this district.
    if (o_id == -1) {
      continue;
    }

    auto order = delivery_getCId(o_id, d_id, arg.w_id);
    if (order == NULL) {
      return finish(Abort);
    }
    uint64_t c_id;
    order->get_value(O_C_ID, c_id);

    delivery_updateOrders(order, arg.o_carrier_id);
            //2.update order
#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
      if (retire_row(row_cnt-1) == Abort){
          return finish(Abort);
      }
#endif
#if CC_ALG == REBIRTH_RETIRE && PASSIVE_RETIRE
    accesses[row_cnt-1]->lock_entry->has_write = true;
#endif

    double ol_total;
// #ifndef TPCC_CAVALIA_NO_OL_UPDATE
    if (!delivery_updateOrderLine_sumOLAmount(arg.ol_delivery_d, o_id, d_id,
                                              arg.w_id, &ol_total)) {
      return finish(Abort);
    }


    if (!delivery_updateCustomer(ol_total, c_id, d_id, arg.w_id)) {
      return finish(Abort);
    }
    //4.update customer
    #if CC_ALG == BAMBOO && (THREAD_CNT != 1)
      if (retire_row(row_cnt-1) == Abort){
          return finish(Abort);
      }
    #endif
    #if CC_ALG == REBIRTH_RETIRE && PASSIVE_RETIRE
      accesses[row_cnt-1]->lock_entry->has_write = true;
    #endif
  }

  auto rc = finish(RCOK);
  return rc;
}

RC tpcc_txn_man::run_order_status(tpcc_query * query) {
///*	row_t * r_cust;
      auto& arg = query->args.order_status;
      #if CC_ALG == REBIRTH_RETIRE
      this->is_long = true;
      #endif

      auto c_id = arg.c_id;
      auto w_id = arg.w_id;
      row_t* customer = NULL;
      if (!arg.by_last_name)
        customer = order_status_getCustomerByCustomerId(arg.w_id, arg.d_id, arg.c_id);
      else
        customer = order_status_getCustomerByLastName(arg.w_id, arg.d_id, arg.c_last, &c_id);

      if (customer == NULL) {
        return finish(Abort);
      };

      auto order = order_status_getLastOrder(arg.w_id, arg.d_id, c_id);
      if (order != NULL) {
        int64_t o_id;
        order->get_value(O_ID, o_id);
        if (!order_status_getOrderLines(arg.w_id, arg.d_id, o_id)) {
            return finish(Abort);
        }
      }

    auto rc = finish(RCOK);
    return RCOK;
}

RC tpcc_txn_man::run_stock_level(tpcc_query * query) {
  auto& arg = query->args.stock_level;
      #if CC_ALG == REBIRTH_RETIRE
      this->is_long = true;
      #endif

  auto district = stock_level_getOId(arg.w_id, arg.d_id);
  if (district == NULL) {
    return finish(Abort);
  }
  int64_t o_id;
  district->get_value(D_NEXT_O_ID, o_id);

  // o_id = o_ids[arg.w_id][arg.d_id];

  uint64_t distinct_count;
  if (!stock_level_getStockCount(arg.w_id, arg.d_id, o_id, arg.w_id,
                                 arg.threshold, &distinct_count)) {
    return finish(Abort);
  }
  (void)distinct_count;

  auto rc = finish(RCOK);
  return rc;
}

row_t* tpcc_txn_man::order_status_getCustomerByCustomerId(uint64_t w_id,
                                                          uint64_t d_id,
                                                          uint64_t c_id) {
  // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
  auto index = _wl->i_customer_id;
  auto key = custKey(c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
  return search(index, key, part_id, RD);
}

row_t* tpcc_txn_man::order_status_getCustomerByLastName(uint64_t w_id,
                                                        uint64_t d_id,
                                                        char* c_last,
                                                        uint64_t* out_c_id) {
// SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST
  auto index = _wl->i_customer_last;
  auto key = custNPKey(c_last, d_id, w_id);
  auto part_id = wh_to_part(w_id);

  itemid_t* items[100];
  size_t count = 100;
  auto rc = index_read_multiple(index, key, items, count, part_id);
  if (rc != RCOK) {
    assert(false);
    return NULL;
  }
  if (count == 0) return NULL;

  auto mid = items[count / 2];
  auto local = get_row((row_t *)mid->location, RD);
  if (local != NULL) local->get_value(C_ID, *out_c_id);
  return local;
}

row_t* tpcc_txn_man::order_status_getLastOrder(uint64_t w_id, uint64_t d_id,
                                               uint64_t c_id) {
  // SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1
  auto index = _wl->i_order_cust;
  auto key = orderCustKey(g_max_orderline, c_id, d_id, w_id);
  auto max_key = orderCustKey(1, c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);

  itemid_t* items[1];
  uint64_t count = 1;

  auto idx_rc = index_read_range(index, key, max_key, items, count, part_id);
  if (idx_rc == Abort) return NULL;
  assert(idx_rc == RCOK);

  // printf("order_status_getLastOrder: %" PRIu64 "\n", count);
  if (count == 0) {
    assert(false);
    return NULL;
  }

  auto shared = items[0];
  auto local = get_row((row_t *)shared->location, RD);
  if (local == NULL) return NULL;
  return local;
}

bool tpcc_txn_man::order_status_getOrderLines(uint64_t w_id, uint64_t d_id,
                                              int64_t o_id) {
  // SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?
  auto index = _wl->i_orderline;
  auto key = orderlineKey(1, o_id, d_id, w_id);
  auto max_key = orderlineKey(15, o_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);

  itemid_t* items[16];
  uint64_t count = 16;

  auto idx_rc = index_read_range(index, key, max_key, items, count, part_id);
  if (idx_rc == Abort) return false;
  assert(idx_rc == RCOK);
  assert(count != 16);

  for (uint64_t i = 0; i < count; i++) {
    auto shared = items[i];
	auto local = get_row((row_t *)shared->location, RD);
    if (local == NULL) {
      return false;
    }

    (void)local;
  }

  return true;
}
bool tpcc_txn_man::delivery_updateCustomer(double ol_total, uint64_t c_id,
                                           uint64_t d_id, uint64_t w_id) {
  // UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?
  auto index = _wl->i_customer_id;
  auto key = custKey(c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
  auto row = search(index, key, part_id, WR);
  if (row == NULL) return false;

  double c_balance;
  uint64_t c_delivery_cnt;
  row->get_value(C_BALANCE, c_balance);
  row->set_value(C_BALANCE, c_balance + ol_total);
//  row->get_value(C_DELIVERY_CNT, c_delivery_cnt);
//  row->set_value(C_DELIVERY_CNT, c_delivery_cnt + 1);

  return true;
}
inline bool tpcc_txn_man::delivery_getNewOrder_deleteNewOrder(uint64_t d_id,
                                                       uint64_t w_id,
                                                       int64_t &out_o_id) {
  // SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1
  // DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?

  auto index = _wl->i_neworder;
  // TODO: This may cause a match with other district with a negative order ID.  It is safe for now because the lowest order ID is 1, but we should give more gap (or use tuple keys) to avoid accidental matches.
  auto key = neworderKey(g_max_orderline, d_id, w_id);
  auto max_key = neworderKey(0, d_id, w_id);  // Use key ">= 0" for "> -1"
  auto part_id = wh_to_part(w_id);

  itemid_t* items[1];
  uint64_t count = 1;

  auto idx_rc = index_read_range_rev(index, key, max_key, items, count, part_id);
  if (idx_rc == Abort) return false;
  assert(idx_rc == RCOK);

  // No new order; this is acceptable and we do not need to abort TX.
  if (count == 0) {
    out_o_id = -1;
    return true;
  }

  auto shared = items[0];
  auto local = get_row((row_t *)shared->location, WR);
  if (local == NULL) return false;

  int64_t o_id;
  local->get_value(NO_O_ID, o_id);
  out_o_id = o_id;

  {
    auto idx = _wl->i_neworder;
    auto key = neworderKey(o_id, d_id, w_id);
	if (!remove_idx(idx, key, (row_t *)items[0]->location, part_id)) return false;

	if (!remove_row((row_t *)shared->location)) return false;
  }

  return true;
}

row_t* tpcc_txn_man::delivery_getCId(int64_t no_o_id, uint64_t d_id,
                                     uint64_t w_id) {
  // SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
  auto index = _wl->i_order;
  auto key = orderKey(no_o_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
  return search(index, key, part_id, WR);
}

void tpcc_txn_man::delivery_updateOrders(row_t* row, uint64_t o_carrier_id) {
  // UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
  row->set_value(O_CARRIER_ID, o_carrier_id);
}
bool tpcc_txn_man::delivery_updateOrderLine_sumOLAmount(uint64_t o_entry_d,
                                                        int64_t no_o_id,
                                                        uint64_t d_id,
                                                        uint64_t w_id,
                                                        double* out_ol_total) {
  // UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?
  // SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
  double ol_total = 0.0;

  auto index = _wl->i_orderline;
  auto key = orderlineKey(1, no_o_id, d_id, w_id);
  auto max_key = orderlineKey(15, no_o_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);

  itemid_t* items[16];
  uint64_t count = 16;

  auto idx_rc = index_read_range(index, key, max_key, items, count, part_id);
  if (idx_rc != RCOK) return false;
  assert(count != 16);

  for (uint64_t i = 0; i < count; i++) {
    auto shared = items[i];
	auto local = get_row((row_t *)shared->location, WR);
    if (local == NULL) return false;
    double ol_amount;
    local->get_value(OL_AMOUNT, ol_amount);
    local->set_value(OL_DELIVERY_D, o_entry_d);
    ol_total += ol_amount;

    //3.update orderline
    #if CC_ALG == BAMBOO && (THREAD_CNT != 1)
      if (retire_row(row_cnt-1) == Abort){
          return finish(Abort);
      }
    #endif
    #if CC_ALG == REBIRTH_RETIRE && PASSIVE_RETIRE
      accesses[row_cnt-1]->lock_entry->has_write = true;
    #endif
  }

  // printf("delivery_updateOrderLine_sumOLAmount: w_id=%" PRIu64 " d_id=%" PRIu64
  //        " o_id=%" PRIu64 " cnt=%" PRIu64 "\n",
  //        w_id, d_id, no_o_id, cnt);
  *out_ol_total = ol_total;
  return true;
}

row_t* tpcc_txn_man::stock_level_getOId(uint64_t d_w_id, uint64_t d_id) {
  // SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?
  auto index = _wl->i_district;
  auto key = distKey(d_id, d_w_id);
  auto part_id = wh_to_part(d_w_id);
  return search(index, key, part_id, RD);
}

bool tpcc_txn_man::stock_level_getStockCount(uint64_t ol_w_id, uint64_t ol_d_id,
                                             int64_t ol_o_id, uint64_t s_w_id,
                                             uint64_t threshold,
                                             uint64_t* out_distinct_count) {

  uint64_t n_orders = 20;
  uint64_t count = n_orders * 15 + 1;

  uint64_t ol_i_id_list[count];
  size_t list_size = 0;

  auto index = _wl->i_orderline;
  auto key = orderlineKey(1, ol_o_id - 1, ol_d_id, ol_w_id);
  auto max_key = orderlineKey(15, ol_o_id - n_orders, ol_d_id, ol_w_id);
  auto part_id = wh_to_part(ol_w_id);

  itemid_t* items[count];

  auto idx_rc = index_read_range(index, key, max_key, items, count, part_id);
  if (idx_rc == Abort) {
      return false;
  }
  assert(idx_rc == RCOK);

  for (uint64_t i = 0; i < count; i++) {
    auto orderline_shared = items[i];
    auto orderline_shared_row = (row_t *)orderline_shared->location;
    if (orderline_shared_row->is_deleted) continue;
    auto orderline = get_row(orderline_shared_row, RD);
    if (orderline == NULL) {
        return false;
    }

    uint64_t ol_i_id, ol_supply_w_id;
    orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
    if (ol_supply_w_id != s_w_id) continue;

    orderline->get_value(OL_I_ID, ol_i_id);
    assert(list_size < sizeof(ol_i_id_list) / sizeof(ol_i_id_list[0]));
    ol_i_id_list[list_size] = ol_i_id;
    list_size++;
  }
  assert(list_size <= count);

  uint64_t distinct_ol_i_id_list[count];
  uint64_t distinct_ol_i_id_count = 0;
  uint64_t result = 0;

  for (uint64_t i = 0; i < list_size; i++) {
    uint64_t ol_i_id = ol_i_id_list[i];

    bool duplicate = false;
    for (uint64_t j = 0; j < distinct_ol_i_id_count; j++)
      if (distinct_ol_i_id_list[j] == ol_i_id) {
        duplicate = true;
        break;
      }
    if (duplicate) continue;

    distinct_ol_i_id_list[distinct_ol_i_id_count++] = ol_i_id;

    auto key = stockKey(ol_i_id, s_w_id);
    auto index = _wl->i_stock;
    auto part_id = wh_to_part(s_w_id);
	auto row = search(index, key, part_id, RD);
    if (row == NULL){
        return false;
    }

    uint64_t s_quantity;
    row->get_value(S_QUANTITY, s_quantity);
    if (s_quantity < threshold) result++;
  }

  *out_distinct_count = result;
  return true;
}
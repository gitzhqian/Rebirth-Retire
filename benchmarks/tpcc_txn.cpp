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
        case TPCC_QUERY2 :
            return run_query2(m_query); break;
        case TPCC_NEW_ORDER :
            return run_new_order(m_query); break;
        case TPCC_ORDER_STATUS :
            return run_order_status(m_query); break;
        case TPCC_DELIVERY :
            return run_delivery(m_query); break;
        case TPCC_STOCK_LEVEL :
            return run_stock_level(m_query); break;
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

    uint64_t w_id = query->w_id;
    uint64_t c_w_id = query->c_w_id;
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
    //use index to retrieve that warehouse
    key = query->w_id;
    INDEX * index = _wl->i_warehouse;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    r_wh = ((row_t *)item->location);
#if !COMMUTATIVE_OPS
    r_wh_local = get_row(r_wh, WR);
#else
    r_wh_local = get_row(r_wh, RD);
#endif
    if (r_wh_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
//        assert(ret);
        return finish(Abort);
#else
        return finish(Abort);
#endif
    }

#if !COMMUTATIVE_OPS
    //update the balance to the warehouse
    r_wh_local->get_value(W_YTD, tmp_value);
    if (g_wh_update) {
        r_wh_local->set_value(W_YTD, tmp_value + query->h_amount);
    }
#else
    inc_value(W_YTD, query->h_amount); // will increment at commit time
#endif
    // bamboo: retire lock for wh
#if (CC_ALG == BAMBOO) && (THREAD_CNT > 1) && !COMMUTATIVE_OPS
    RETIRE_ROW(row_cnt)
#endif
    //get a copy of warehouse name
    tmp_str = r_wh_local->get_value(W_NAME);
    memcpy(w_name, tmp_str, 10);
    w_name[10] = '\0';
#if CC_ALG == IC3
    if (end_piece(0) != RCOK)
        goto warehouse_piece;
#endif
    //END: [WAREHOUSE] RW

    //BEGIN: [DISTRICT] RW
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
    key = distKey(query->d_id, query->d_w_id);
    item = index_read(_wl->i_district, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_dist = ((row_t *)item->location);
#if !COMMUTATIVE_OPS
    row_t * r_dist_local = get_row(r_dist, WR);
#else
    row_t * r_dist_local = get_row(r_dist, RD);
#endif
    if (r_dist_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
        assert(ret);
        return finish(Abort);
#else
        return finish(Abort);
#endif
    }
#if !COMMUTATIVE_OPS
    r_dist_local->get_value(D_YTD, tmp_value);
    r_dist_local->set_value(D_YTD, tmp_value + query->h_amount);
#else
    inc_value(D_YTD, query->h_amount); // will increment at commit time
#endif
#if (CC_ALG == BAMBOO) && (THREAD_CNT > 1) && !COMMUTATIVE_OPS
    RETIRE_ROW(row_cnt)
#endif
    tmp_str = r_dist_local->get_value(D_NAME);
    memcpy(d_name, tmp_str, 10);
    d_name[10] = '\0';
#if CC_ALG == IC3
    if(end_piece(1) != RCOK)
        goto district_piece;
#endif
    //END: [DISTRICT] RW

    //BEGIN: [CUSTOMER] RW
#if CC_ALG == IC3
    customer_piece:
    begin_piece(2);
#endif
    if (query->by_last_name) {
        /*==========================================================+
            EXEC SQL SELECT count(c_id) INTO :namecnt
            FROM customer
            WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
        +==========================================================*/
        /*==========================================================================+
            EXEC SQL DECLARE c_byname CURSOR FOR
            SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
            c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
            FROM customer
            WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
            ORDER BY c_first;
            EXEC SQL OPEN c_byname;
        +===========================================================================*/
        /*============================================================================+
        for (n=0; n<namecnt/2; n++) {
            EXEC SQL FETCH c_byname
            INTO :c_first, :c_middle, :c_id,
                 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
                 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
        }
        EXEC SQL CLOSE c_byname;
    +=============================================================================*/
        // XXX: we don't retrieve all the info, just the tuple we are interested in
        uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
        // XXX: the list is not sorted. But let's assume it's sorted...
        // The performance won't be much different.
        INDEX * index = _wl->i_customer_last;
        item = index_read(index, key, wh_to_part(c_w_id));
        assert(item != NULL);
        cnt = 0;
        itemid_t * it = item;
        itemid_t * mid = item;
        while (it != NULL) {
            cnt ++;
            it = it->next;
            if (cnt % 2 == 0)
                mid = mid->next;
        }
        //get the center one, as in spec
        r_cust = ((row_t *)mid->location);
    }
    else { // search customers by cust_id
        /*=====================================================================+
            EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
            c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
            c_discount, c_balance, c_since
            INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
            :c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
            :c_discount, :c_balance, :c_since
            FROM customer
            WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
        +======================================================================*/
        key = custKey(query->c_id, query->c_d_id, query->c_w_id);
        INDEX * index = _wl->i_customer_id;
        item = index_read(index, key, wh_to_part(c_w_id));
        assert(item != NULL);
        r_cust = (row_t *) item->location;
    }
    /*======================================================================+
         EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
         WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
     +======================================================================*/
    r_cust_local = get_row(r_cust, WR);
    if (r_cust_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
        assert(ret);
        return finish(Abort);
#else
        return finish(Abort);
#endif
    }
    r_cust_local->get_value(C_BALANCE, c_balance);
    r_cust_local->set_value(C_BALANCE, c_balance - query->h_amount);
    r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
    r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
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
        sprintf(c_new_data,"| %zu %zu %zu %zu %zu $%7.2f %d",
                query->c_id, query->c_d_id, c_w_id, query->d_id, w_id, query->h_amount,'\0');
        //char * c_data = r_cust->get_value("C_DATA");
        //need to fix this line
        //strncat(c_new_data, c_data, 500 - strlen(c_new_data));
        //c_new_data[500]='\0';
        r_cust->set_value("C_DATA", c_new_data);
    }
#if (CC_ALG == BAMBOO) && (THREAD_CNT > 1)
    RETIRE_ROW(row_cnt)
#endif

#if CC_ALG == IC3
    if(end_piece(2) != RCOK)
        goto customer_piece;
#endif
    //END: [CUSTOMER] - RW

    //START: [HISTORY] - WR
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
    r_hist->set_value(H_C_ID, query->c_id);
    r_hist->set_value(H_C_D_ID, query->c_d_id);
    r_hist->set_value(H_C_W_ID, c_w_id);
    r_hist->set_value(H_D_ID, query->d_id);
    r_hist->set_value(H_W_ID, w_id);
    int64_t date = 2013;
    r_hist->set_value(H_DATE, date);
    r_hist->set_value(H_AMOUNT, query->h_amount);
#if !TPCC_SMALL
    r_hist->set_value(H_DATA, h_data);
#endif
    // XXX(zhihan): no index maintained for history table.
    //END: [HISTORY] - WR
    assert( rc == RCOK );

#if CC_ALG == REBIRTH_RETIRE
    auto ret = ATOM_CAS(status, RUNNING, validating);
    assert(ret);
    return finish(rc);
#else
    return finish(rc);
#endif
}


/**
 * 1. Read WAREHOUSE, DISTRICT, CUSTOMER table.
 * 2. Insert a tuple in NEW_ORDER, Order table.
 * 3. Read ol_cnt tuples in ITEM, STOCK table.
 * @param query
 * @return
 */
RC tpcc_txn_man::run_new_order(tpcc_query * query) {
    RC rc = RCOK;
    uint64_t key;
    itemid_t * item;
    INDEX * index;

    bool remote = query->remote;
    uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
    uint64_t ol_cnt = query->ol_cnt;

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
    INDEX * stock_index;
    itemid_t * stock_item;
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
    key = w_id;
    index = _wl->i_warehouse;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local = get_row(r_wh, RD);
    if (r_wh_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
        assert(ret);
        return finish(Abort);
#else
        return finish(Abort);
#endif
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
    key = distKey(d_id, w_id);
    item = index_read(_wl->i_district, key, wh_to_part(w_id));
    assert(item != NULL);
    r_dist = ((row_t *)item->location);
    r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
        assert(ret);
        return finish(Abort);
#else
        return finish(Abort);
#endif
    }

    //d_tax = *(double *) r_dist_local->get_value(D_TAX);
    r_dist_local->get_value(D_TAX);
    uint64_t o_id;
    r_dist_local->get_value(D_NEXT_O_ID, o_id);
//  o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);

    o_id ++;
    r_dist_local->set_value(D_NEXT_O_ID, o_id);

#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
    if (retire_row(row_cnt-1) == Abort){
        return finish(Abort);
    }
#endif

#if CC_ALG == IC3
    if (end_piece(1) != RCOK)
        goto district_piece;

    customer_piece:
    begin_piece(2);
#endif
    //select customer
    key = custKey(c_id, d_id, w_id);
    index = _wl->i_customer_id;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    r_cust = (row_t *) item->location;
    r_cust_local = get_row(r_cust, RD);
    if (r_cust_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
        assert(ret);
        return finish(Abort);
#else
        return finish(Abort);
#endif
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
    _wl->t_neworder->get_new_row(r_no, 0, row_id);
    uint64_t neword_key = neworderKey(o_id, d_id, w_id);
    r_no->set_primary_key(neword_key);
    r_no->set_value(NO_O_ID, o_id);
    r_no->set_value(NO_D_ID, d_id);
    r_no->set_value(NO_W_ID, w_id);
    insert_row(r_no, _wl->t_neworder);

    auto i_neworder = _wl->i_neworder;
    index_insert(r_no, i_neworder,  neword_key);
//  */
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
//  /*
    _wl->t_order->get_new_row(r_order, 0, row_id);
    uint64_t ord_key = orderKey(o_id, d_id, w_id);
    r_order->set_primary_key(ord_key);
    r_order->set_value(O_ID, o_id);
    r_order->set_value(O_C_ID, c_id);
    r_order->set_value(O_D_ID, d_id);
    r_order->set_value(O_W_ID, w_id);
    r_order->set_value(O_ENTRY_D, query->o_entry_d);
    r_order->set_value(O_OL_CNT, ol_cnt);
//  o_d_id=*(int64_t *) r_order->get_value(O_D_ID);
//  o_d_id=d_id;
//  all_local = (remote? 0 : 1);
//  r_order->set_value(O_ALL_LOCAL, all_local);
    insert_row(r_order, _wl->t_order);

    auto i_order = _wl->i_order;
    index_insert(r_order, i_order, ord_key);
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
        ol_i_id = query->items[ol_number].ol_i_id;
#if TPCC_USER_ABORT
        // XXX(zhihan): if key is invalid, abort. user-initiated abort
        // according to tpc-c documentation
        if (ol_i_id == 0)
          return finish(ERROR);
#endif
        ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
        ol_quantity = query->items[ol_number].ol_quantity;
        /*===========================================+
        EXEC SQL SELECT i_price, i_name , i_data
            INTO :i_price, :i_name, :i_data
            FROM item
            WHERE i_id = :ol_i_id;
        +===========================================*/
        key = ol_i_id;
        item = index_read(_wl->i_item, key, 0);
        assert(item != NULL);
        r_item = ((row_t *)item->location);
        r_item_local = get_row(r_item, RD);
        if (r_item_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
//        assert(ret);
        return finish(Abort);
#else
    return finish(Abort);
#endif
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

        stock_key = stockKey(ol_i_id, ol_supply_w_id);
        stock_index = _wl->i_stock;
        index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
        assert(item != NULL);
        r_stock = ((row_t *)stock_item->location);
        r_stock_local = get_row(r_stock, WR);
        if (r_stock_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
//        assert(ret);
        return finish(Abort);
#else
    return finish(Abort);
#endif
        }

        // XXX s_dist_xx are not retrieved.
        s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
        //try to retrieve s_dist_xx
#if !TPCC_SMALL
//        /*
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
//        */
        //char * s_data = "test";
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

#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
        if (retire_row(row_cnt-1) == Abort){
            return finish(Abort);
        }
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
	/*
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
        auto i_orderline = _wl->i_orderline;
        index_insert(r_ol, i_orderline, ordline_key);
	 */
    }

#endif // if CC_ALG == IC3
    assert( rc == RCOK );

#if CC_ALG == REBIRTH_RETIRE
    auto ret = ATOM_CAS(status, RUNNING, validating);
    assert(ret);
    return finish(rc);
#else
    return finish(rc);
#endif
}



RC tpcc_txn_man::run_order_status(tpcc_query * query) {
/*	row_t * r_cust;
	if (query->by_last_name) {
		// EXEC SQL SELECT count(c_id) INTO :namecnt FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
		// EXEC SQL DECLARE c_name CURSOR FOR SELECT c_balance, c_first, c_middle, c_id
		// FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id ORDER BY c_first;
		// EXEC SQL OPEN c_name;
		// if (namecnt%2) namecnt++; / / Locate midpoint customer for (n=0; n<namecnt/ 2; n++)
		// {
		//	   	EXEC SQL FETCH c_name
		//	   	INTO :c_balance, :c_first, :c_middle, :c_id;
		// }
		// EXEC SQL CLOSE c_name;

		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted...
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		uint64_t thd_id = get_thd_id();
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
	} else {
		// EXEC SQL SELECT c_balance, c_first, c_middle, c_last
		// INTO :c_balance, :c_first, :c_middle, :c_last
		// FROM customer
		// WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
		uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		r_cust = (row_t *) item->location;
	}
#if TPCC_ACCESS_ALL

	row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	r_cust_local->get_value(C_BALANCE, c_balance);
	char * c_first = r_cust_local->get_value(C_FIRST);
	char * c_middle = r_cust_local->get_value(C_MIDDLE);
	char * c_last = r_cust_local->get_value(C_LAST);
#endif
	// EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
	// INTO :o_id, :o_carrier_id, :entdate FROM orders
	// ORDER BY o_id DESC;
	uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
	INDEX * index = _wl->i_order;
	itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
	row_t * r_order = (row_t *) item->location;
	row_t * r_order_local = get_row(r_order, RD);
	if (r_order_local == NULL) {
		assert(false);
		return finish(Abort);
	}

	uint64_t o_id, o_entry_d, o_carrier_id;
	r_order_local->get_value(O_ID, o_id);
#if TPCC_ACCESS_ALL
	r_order_local->get_value(O_ENTRY_D, o_entry_d);
	r_order_local->get_value(O_CARRIER_ID, o_carrier_id);
#endif
#if DEBUG_ASSERT
	itemid_t * it = item;
	while (it != NULL && it->next != NULL) {
		uint64_t o_id_1, o_id_2;
		((row_t *)it->location)->get_value(O_ID, o_id_1);
		((row_t *)it->next->location)->get_value(O_ID, o_id_2);
		assert(o_id_1 > o_id_2);
	}
#endif

	// EXEC SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// ol_amount, ol_delivery_d
	// FROM order_line
	// WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// EXEC SQL OPEN c_line;
	// EXEC SQL WHENEVER NOT FOUND CONTINUE;
	// i=0;
	// while (sql_notfound(FALSE)) {
	// 		i++;
	//		EXEC SQL FETCH c_line
	//		INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i], :ol_amount[i], :ol_delivery_d[i];
	// }
	key = orderlineKey(query->w_id, query->d_id, o_id);
	index = _wl->i_orderline;
	item = index_read(index, key, wh_to_part(query->w_id));
	assert(item != NULL);
#if TPCC_ACCESS_ALL
	// TODO the rows are simply read without any locking mechanism
	while (item != NULL) {
		row_t * r_orderline = (row_t *) item->location;
		int64_t ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d;
		r_orderline->get_value(OL_I_ID, ol_i_id);
		r_orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
		r_orderline->get_value(OL_QUANTITY, ol_quantity);
		r_orderline->get_value(OL_AMOUNT, ol_amount);
		r_orderline->get_value(OL_DELIVERY_D, ol_delivery_d);
		item = item->next;
	}
#endif

final:
	assert( rc == RCOK );
	return finish(rc)*/
    return RCOK;
}


//TODO concurrency for index related operations is not completely supported yet.
// In correct states may happen with the current code.

RC
tpcc_txn_man::run_delivery(tpcc_query * query) {
///*
    // XXX HACK if another delivery txn is running on this warehouse, simply commit.
//	if ( !ATOM_CAS(_wl->delivering[query->w_id], false, true) )
//		return finish(RCOK);

#if !TPCC_SMALL
    uint64_t n_o_oid = query->n_o_id;
    uint64_t wid= query->w_id;
    uint64_t part_id = wh_to_part(wid);

    for (int d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
        auto  key_dist = distKey(d_id, wid);
        auto item_dist = index_read(_wl->i_district, key_dist, part_id);
        assert(item != NULL);
        auto r_dist = ((row_t *)item_dist->location);
        auto r_dist_local = get_row(r_dist, RD);
        if (r_dist_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
            auto ret = ATOM_CAS(status, RUNNING, ABORTED);
            assert(ret);
            return finish(Abort);
#else
            return finish(Abort);
#endif
        }
//		uint64_t key = distKey(d_id, query->w_id);
//		INDEX * index = _wl->i_orderline_wd;
//		itemid_t * item = index_read(index, key, wh_to_part(query->w_id));
//		assert(item != NULL);
//		while (item->next != NULL) {
//#if DEBUG_ASSERT
//			uint64_t o_id_1, o_id_2;
//			((row_t *)item->location)->get_value(OL_O_ID, o_id_1);
//			((row_t *)item->next->location)->get_value(OL_O_ID, o_id_2);
//			assert(o_id_1 > o_id_2);
//#endif
//			item = item->next;
//		}
//		row_t * r_orderline = (row_t *)item->location;
//		r_orderling->get_value(OL_O_ID, no_o_id);
//		// TODO the orderline row should be removed from the table and indexes.
        auto max_key = neworderKey(n_o_oid, d_id, wid);
        auto idx_neworder = _wl->i_neworder;
        M_ASSERT(idx_neworder,"new order index is null. \n");
        itemid_t * item_nord = index_read(idx_neworder, max_key, part_id);
        row_t * r_neworder = (row_t *)item_nord->location;
        row_t * r_neworder_local = get_row(r_neworder, RD);
        if (r_neworder_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
            auto ret = ATOM_CAS(status, RUNNING, ABORTED);
            assert(ret);
            return finish(Abort);
#else
            return finish(Abort);
#endif
        }
        uint64_t no_o_id;
        r_neworder_local->get_value(NO_O_ID, no_o_id);

        uint64_t key_ord = orderKey(no_o_id, d_id, query->w_id);
        auto idx_ord = _wl->i_order;
        itemid_t * item_ord = index_read(idx_ord, key_ord, part_id);
        if (item_ord == nullptr){
            printf("item_ord is null, no_o_id:%lu . \n",no_o_id);
            continue;
        }
        row_t * r_order = (row_t *)item_ord->location;
        row_t * r_order_local = get_row(r_order, WR);
        if (r_order_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
            auto ret = ATOM_CAS(status, RUNNING, ABORTED);
            assert(ret);
            return finish(Abort);
#else
            return finish(Abort);
#endif
        }
        uint64_t o_c_id;
        r_order_local->get_value(O_C_ID, o_c_id);
        r_order_local->set_value(O_CARRIER_ID, query->o_carrier_id);
#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
        if (retire_row(row_cnt-1) == Abort)
            return finish(Abort);
#endif

        uint64_t count = 15;
        double sum_ol_amount;
        auto idx_ordline = _wl->i_orderline;
        M_ASSERT(idx_ordline, "orderline index is null, \n");
        for (uint64_t i = 1; i < count; i++) {
            uint64_t key_ordline =  orderlineKey(i, no_o_id, d_id, wid);
            itemid_t * item_ordline = index_read(idx_ordline, key_ordline, part_id);
            double ol_amount;
            if (item_ordline != NULL) {
                // TODO the row is not locked
                row_t * r_orderline = (row_t *)item_ordline->location;
                row_t * r_orderline_local = get_row(r_orderline, WR);
                if (r_orderline_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
                    auto ret = ATOM_CAS(status, RUNNING, ABORTED);
                    assert(ret);
                    return finish(Abort);
#else
                    return finish(Abort);
#endif
                }
                r_orderline->set_value(OL_DELIVERY_D, query->ol_delivery_d);
                r_orderline->get_value(OL_AMOUNT, ol_amount);
                sum_ol_amount += ol_amount;

#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
                if (retire_row(row_cnt-1) == Abort)
                    return finish(Abort);
#endif
            }
        }

        uint64_t key_cust = custKey(o_c_id, d_id, query->w_id);
        itemid_t * item_cust = index_read(_wl->i_customer_id, key_cust, part_id);
        row_t * r_cust = (row_t *)item_cust->location;
        row_t * r_cust_local = get_row(r_cust, WR);
        if (r_cust_local == NULL) {
#if CC_ALG == REBIRTH_RETIRE
            auto ret = ATOM_CAS(status, RUNNING, ABORTED);
            assert(ret);
            return finish(Abort);
#else
            return finish(Abort);
#endif
        }
        double c_balance;
        uint64_t c_delivery_cnt;
        r_cust_local->get_value(C_BALANCE, c_balance);
        r_cust_local->set_value(C_BALANCE, c_balance + sum_ol_amount);
        r_cust_local->get_value(C_DELIVERY_CNT, c_delivery_cnt);
        r_cust_local->set_value(C_DELIVERY_CNT, c_delivery_cnt + 1);

#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
        if (retire_row(row_cnt-1) == Abort)
            return finish(Abort);
#endif
    }

//*/
    return  finish(RCOK);
#endif
}

RC
tpcc_txn_man::run_stock_level(tpcc_query * query) {
    return RCOK;
}


RC tpcc_txn_man::run_query2(tpcc_query * query ) {
    /*  "query_2": {
                   "SELECT su_suppkey, su_name,  n_name, i_id, i_name, su_address, su_phone, "
                    + "su_comment FROM item, supplier, stock, nation, region, "
                    + "(SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity  FROM stock, "
                    + "supplier, nation, region  WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey "
                    + "AND su_nationkey=n_nationkey AND n_regionkey=r_regionkey "
                    + "AND r_name LIKE 'Europ%'  GROUP BY s_i_id) m "
                    + "WHERE i_id = s_i_id  AND MOD((s_w_id * s_i_id), 10000) = su_suppkey "
                    + "AND su_nationkey = n_nationkey  AND n_regionkey = r_regionkey "
                    + "AND i_data LIKE '%b'  AND r_name LIKE 'Europ%' "
                    + "AND i_id=m_i_id  AND s_quantity = m_s_quantity "
                    + "ORDER BY n_name,  su_name,  i_id"
     }  */
//    auto regions_ = _wl->ch_regions;
//    auto nations_ = _wl->ch_nations;
//    auto suppliers_ = _wl->ch_suppliers;
//    auto supplier_item_map = _wl->supp_stock_map;// ,w_id,i_id
//    uint64_t supplier_num = suppliers_.size();
//    // Pick a target region
//    auto target_region = 0;
////    auto target_region = GetRandomInteger(0, 4);
//    //	auto target_region = 3;
//    assert(0 <= target_region and target_region <= 4);
//    auto stock_index = _wl->i_stock;
//
//    uint64_t starttime = get_sys_clock();
//
//    // Scan region
//    for (auto &r_r : regions_) {
//        uint64_t r_id = r_r.second->region_id;
//        std::string r_name = r_r.second->region_name;
//
//        // filtering region
//        if (r_name != std::string(regions[target_region])) continue;
//
//        // Scan nation
//        for (auto &r_n : nations_) {
//            uint64_t n_id = r_n.second->nation_id;
//            std::string n_name = r_n.second->nation_name;
//            // filtering nation
//            if (r_r.second->region_id != r_n.second->region_id) continue;
//            // Scan suppliers
//            for (uint64_t i = 0; i < supplier_num; i++) {
//                auto suppll = suppliers_.find(i);
//                if (r_n.second->nation_id != suppll->second->su_nation_id) continue;
//
//                int64_t min_w_id = 0;
//                int64_t min_i_id = 0;
//                int64_t min_s_quant = 0;
//                int64_t min_s_ytd = 0;
//                int64_t min_s_ord_cnt = 0;
//                int64_t min_s_rem_cnt = 0;
//
//                // aggregate - finding a stock tuple having min. stock level
//                int16_t min_qty = std::numeric_limits<int16_t>::max();
//                // "mod((s_w_id*s_i_id),10000)=su_suppkey"
//                // items
//                int64_t supp_id = suppll->second->su_supp_id;
//                for (auto &it : supplier_item_map[supp_id])  // already know
//                {
//                    uint64_t ol_i_id = it.second;
//                    uint64_t ol_supply_w_id = it.first;
//
//                    uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id); //ol_i_id, ol_supply_w_id
//                    stock_index = _wl->i_stock;
//                    itemid_t * stock_item;
//                    index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
//
//                    auto r_stock = ((row_t *)stock_item->location);
//                    auto r_stock_local = get_row(r_stock, RD);
//                    if (r_stock_local == NULL) {
//#if CC_ALG == REBIRTH_RETIRE
//                        auto ret = ATOM_CAS(status, RUNNING, ABORTED);
//                        assert(ret);
//                        return finish(Abort);
//#else
//                        return finish(Abort);
//#endif
//                    }
//
//                    int64_t s_w_id, s_i_id ,s_quantity ,s_ytd, s_order_cnt, s_remote_cnt ;
//                    r_stock_local->get_value(S_W_ID, s_w_id);
//                    r_stock_local->get_value(S_I_ID, s_i_id);
//                    r_stock_local->get_value(S_QUANTITY, s_quantity);
//#if !TPCC_SMALL
//                    r_stock_local->get_value(S_YTD, s_ytd);
//                    r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
//#endif
//                    r_stock_local->get_value(S_REMOTE_CNT, s_remote_cnt);
//                    //ASSERT(s_w_id * s_i_id % 10000 == supp_id);
//                    if (min_qty > s_quantity) {
//                        min_w_id = s_w_id;
//                        min_i_id = s_i_id;
//                        min_s_quant = s_quantity;
//                        min_s_ytd = s_ytd;
//                        min_s_ord_cnt = s_order_cnt;
//                        min_s_rem_cnt = s_remote_cnt;
//                    }
//                }
//
//                // fetch the (lowest stock level) item info
//                auto index = _wl->i_item;
//                uint64_t key =  min_i_id;
//                auto part_id = 0;
//                auto item_ = index_read(_wl->i_item, key, 0);
//
//                auto r_item = ((row_t *)item_->location);
//                auto item_row = get_row(r_item, RD);
//                if (item_row == NULL) {
//#if CC_ALG == REBIRTH_RETIRE
//                    auto ret = ATOM_CAS(status, RUNNING, ABORTED);
//                    assert(ret);
//                    return finish(Abort);
//#else
//                    return finish(Abort);
//#endif
//                }

//                if (item_row == NULL) continue;
//                char *item_data = item_row->data;
//                char *i_data = item_data + 48;
//                std::string str_i_data = std::string(i_data, 50);
//                //  filtering item (i_data like '%b')
//                auto found = str_i_data.find('b');
//                if (found != std::string::npos) continue;

                // XXX. read-mostly txn: update stock or item here

//                if (min_s_quant < 100) {
//                    uint64_t stock_k = stockKey(min_i_id, min_w_id); //item_id, warehouse_id
//                    auto part_id = wh_to_part(min_w_id);
//                    stock_index = _wl->i_stock;
//                    itemid_t * stock_item;
//                    index_read(stock_index, stock_k, wh_to_part(min_w_id), stock_item);
//
//                    auto r_stock = ((row_t *)stock_item->location);
//                    auto r_stock_local = get_row(r_stock, WR);
//                    if (r_stock_local == NULL) {
//                    #if CC_ALG == REBIRTH_RETIRE
//                            auto ret = ATOM_CAS(status, RUNNING, ABORTED);
//                            assert(ret);
//                            return finish(Abort);
//                    #else
//                        return finish(Abort);
//                    #endif
//                    }
//
//                    r_stock_local->set_value(S_QUANTITY, min_s_quant+50);
//                    r_stock_local->set_value(S_YTD, min_s_ytd);
//                    r_stock_local->set_value(S_ORDER_CNT, min_s_ord_cnt);
//                    r_stock_local->set_value(S_REMOTE_CNT, min_s_rem_cnt);
//
//                    printf("update stock. \n");
//
//                    #if CC_ALG == BAMBOO && (THREAD_CNT != 1)
//                      if (retire_row(row_cnt-1) == Abort)
//                          return finish(Abort);
//                    #endif
//                }

                /*  cout << k_su.su_suppkey              << ","
                         << v_su->su_name                << ","
                         << v_n->n_name                  << ","
                         << k_i.i_id                     << ","
                         << v_i->i_name                  << std::endl;  */
//            }
//        }
//    }
//
//    uint64_t timespan = get_sys_clock() - starttime;
////    INC_STATS(get_thd_id(), time_q2, timespan);
//
//    return finish(RCOK);
}

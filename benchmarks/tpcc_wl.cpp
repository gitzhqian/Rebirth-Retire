#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"

RC tpcc_wl::init() {
    workload::init();
    string path = "./benchmarks/";
#if TPCC_SMALL
    path += "TPCC_short_schema.txt";
#else
    path += "TPCC_full_schema.txt";
#endif

    cout << "reading schema file: " << path << endl;
    init_schema( path.c_str() );
    cout << "TPCC schema initialized" << endl;
    init_table();
    next_tid = 0;
    ASSERT(g_perc_neworder >= 0);
#if CC_ALG == IC3
    init_scgraph();
#endif
    return RCOK;
}

RC tpcc_wl::init_schema(const char * schema_file) {
    workload::init_schema(schema_file);
    t_warehouse = tables["WAREHOUSE"];
    t_district = tables["DISTRICT"];
    t_customer = tables["CUSTOMER"];
    t_history = tables["HISTORY"];
    t_neworder = tables["NEW-ORDER"];
    t_order = tables["ORDER"];
    t_orderline = tables["ORDER-LINE"];
    t_item = tables["ITEM"];
    t_stock = tables["STOCK"];

    i_item = indexes["ITEM_IDX"];
    i_warehouse = indexes["WAREHOUSE_IDX"];
    i_district = indexes["DISTRICT_IDX"];
    i_customer_id = indexes["CUSTOMER_ID_IDX"];
    i_customer_last = indexes["CUSTOMER_LAST_IDX"];
    i_stock = indexes["STOCK_IDX"];

    i_order = indexes["ORDER_IDX"];
    i_neworder = indexes["NEWORDER_IDX"];
    i_orderline = indexes["ORDERLINE_IDX"];

    return RCOK;
}

RC tpcc_wl::init_table() {
    num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order
//		- new order
//		- order line
/**********************************/
    tpcc_buffer = new drand48_data * [g_num_wh];
    pthread_t * p_thds = new pthread_t[g_num_wh - 1];
    for (uint32_t i = 0; i < g_num_wh - 1; i++) {
        pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
    }
    threadInitWarehouse(this);
    for (uint32_t i = 0; i < g_num_wh - 1; i++) {
        pthread_join(p_thds[i], NULL);
    }

    tpcc_wl * wl = (tpcc_wl *)this;
//    wl->init_tab_nation();
//    wl->init_tab_region();
//    wl->init_tab_supplier();
    // value ranges 0 ~ 9999 ( modulo by 10k )
//    supp_stock_map.resize(10000);
//    // pre-build supp-stock mapping table to boost tpc-ch queries
//    for (uint w = 1; w <= g_num_wh; w++){
//        for (uint i = 1; i <= g_max_items; i++){
//            supp_stock_map[w * i % 10000].push_back(std::make_pair(w, i));
//        }
//    }

    printf("TPCC Data Initialization Complete!\n");
    return RCOK;
}

RC tpcc_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
    txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), 64);
    new(txn_manager) tpcc_txn_man();
    txn_manager->init(h_thd, this, h_thd->get_thd_id());
    return RCOK;
}

void tpcc_wl::init_tab_region() {
//    for (uint64_t i = 0; i < 5; ++i) {
//    for (uint64_t i = 0; i < 1; ++i) {
//        uint64_t r_id = i;
//        std::string r_name = std::string(regions[i]);
//        std::string r_comment = GetRandomAlphaNumericString(152);
//
//        Region *region = new Region{r_id, r_name, r_comment};
//        ch_regions.insert(std::make_pair(r_id, region));
//    }
}
void tpcc_wl::init_tab_nation() {
//    for (uint64_t i = 0; i < 62; i++) {
//    for (uint64_t i = 0; i < 1; i++) {
//        uint64_t n_id = i;
//        uint64_t r_id = nations[n_id].region_id;
//        std::string n_name = nations[n_id].nation_name;
//        std::string n_comment = GetRandomAlphaNumericString(152);
//
//        Nation *nation = new Nation{n_id, r_id, n_name, n_comment};
//        ch_nations.insert(std::make_pair(n_id, nation));
//    }
}
void tpcc_wl::init_tab_supplier() {
//    for (uint64_t i = 0; i < 10000; ++i) {
//        uint64_t supp_id = i;
//        uint64_t su_nation_id = GetRandomInteger(0, 61);
//        double su_acctbal = GetRandomDouble(0.0, 0.2);
//        std::string su_name = std::string("Supplier#") + std::string("000000000") + std::to_string(supp_id);;
//        std::string su_address = GetRandomAlphaNumericString(40);; //40
//        std::string su_phone = GetRandomAlphaNumericString(15);;  //15
//        std::string su_comment = GetRandomAlphaNumericString(15);; //15
//
//        Supplier *supplier = new Supplier{supp_id, su_nation_id, su_acctbal,
//                                          su_name, su_address, su_phone, su_comment };
//        ch_suppliers.insert(std::make_pair(supp_id, supplier));
//    }
}

// TODO ITEM table is assumed to be in partition 0
/**
 * Thread 0 will insert all items into partition 0.
 * item count = g_max_items = 100000. (defined in global.cpp)
 */
void tpcc_wl::init_tab_item() {
    for (uint64_t i = 1; i <= g_max_items; i++) {
        row_t * row;
        uint64_t row_id;        // row_id should be never used, because it isn't initialized.
        t_item->get_new_row(row, 0, row_id);
        row->set_primary_key(i);
        row->set_value(I_ID, i);
        row->set_value(I_IM_ID, URand(1L,10000L, 0));
        char name[24];
        MakeAlphaString(14, 24, name, 0);
        row->set_value(I_NAME, name);
        row->set_value(I_PRICE, URand(1, 100, 0));
        char data[50];
        MakeAlphaString(26, 50, data, 0);
        // TODO in TPCC, "original" should start at a random position
        if (RAND(10, 0) == 0)
            strcpy(data, "original");
        row->set_value(I_DATA, data);

        index_insert(i_item, i, row, 0);
    }
}

/**
 * Every thread only insert 1 tuple in WAREHOUSE table.
 * @param wid : the primary key of the tuple to be inserted
 */
void tpcc_wl::init_tab_wh(uint32_t wid) {
    assert(wid >= 1 && wid <= g_num_wh);
    row_t * row;
    uint64_t row_id;
    t_warehouse->get_new_row(row, 0, row_id);
    row->set_primary_key(wid);

    row->set_value(W_ID, wid);
    char name[10];
    MakeAlphaString(6, 10, name, wid-1);
    row->set_value(W_NAME, name);
    char street[20];
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_STREET_1, street);
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_STREET_2, street);
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_CITY, street);
    char state[2];
    MakeAlphaString(2, 2, state, wid-1); /* State */
    row->set_value(W_STATE, state);
    char zip[9];
    MakeNumberString(9, 9, zip, wid-1); /* Zip */
    row->set_value(W_ZIP, zip);
    double tax = (double)URand(0L,200L,wid-1)/1000.0;
    double w_ytd=300000.00;
    row->set_value(W_TAX, tax);
    row->set_value(W_YTD, w_ytd);

    index_insert(i_warehouse, wid, row, wh_to_part(wid));
    return;
}


/**
 * Every thread insert 10 tuples in DISTRICT table.
 * DIST_PER_WARE: 10 (defined in config.h)
 * @param wid : the corresponding WAREHOUSE id.
 */
void tpcc_wl::init_tab_dist(uint64_t wid) {
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
        row_t * row;
        uint64_t row_id;
        t_district->get_new_row(row, 0, row_id);
        row->set_primary_key(did);

        row->set_value(D_ID, did);
        row->set_value(D_W_ID, wid);
        char name[10];
        MakeAlphaString(6, 10, name, wid-1);
        row->set_value(D_NAME, name);
        char street[20];
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_CITY, street);
        char state[2];
        MakeAlphaString(2, 2, state, wid-1); /* State */
        row->set_value(D_STATE, state);
        char zip[9];
        MakeNumberString(9, 9, zip, wid-1); /* Zip */
        row->set_value(D_ZIP, zip);
        double tax = (double)URand(0L,200L,wid-1)/1000.0;
        double w_ytd=30000.00;
        row->set_value(D_TAX, tax);
        row->set_value(D_YTD, w_ytd);
        row->set_value(D_NEXT_O_ID, 3001);

        index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));
    }
}


/**
 * Every thread insert 100000 tuples in STOCK table.
 * g_max_items: 100000 (defined in global.cpp)
 * @param wid : the corresponding WAREHOUSE id.
 */
void tpcc_wl::init_tab_stock(uint64_t wid) {

    for (uint64_t sid = 1; sid <= g_max_items; sid++) {
        row_t * row;
        uint64_t row_id;
        t_stock->get_new_row(row, 0, row_id);
        row->set_primary_key(sid);
        row->set_value(S_I_ID, sid);
        row->set_value(S_W_ID, wid);
        row->set_value(S_QUANTITY, URand(10, 100, wid-1));
        row->set_value(S_REMOTE_CNT, 0);
#if !TPCC_SMALL
        char s_dist[25];
        char row_name[10] = "S_DIST_";
        for (int i = 1; i <= 10; i++) {
            if (i < 10) {
                row_name[7] = '0';
                row_name[8] = i + '0';
            } else {
                row_name[7] = '1';
                row_name[8] = '0';
            }
            row_name[9] = '\0';
            MakeAlphaString(24, 24, s_dist, wid-1);
            row->set_value(row_name, s_dist);
        }
        row->set_value(S_YTD, 0);
        row->set_value(S_ORDER_CNT, 0);
        char s_data[50];
        int len = MakeAlphaString(26, 50, s_data, wid-1);
        if (rand() % 100 < 10) {
            int idx = URand(0, len - 8, wid-1);
            strcpy(&s_data[idx], "original");
        }
        row->set_value(S_DATA, s_data);
#endif
        index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));
    }
}


/**
 * Every thread insert 3000 tuples in CUSTOMER table.
 * g_cust_per_dist: 3000 (defined in global.cpp)
 * @param did : the corresponding DISTRICT id.
 * @param wid : the corresponding WAREHOUSE id.
 */
void tpcc_wl::init_tab_cust(uint64_t did, uint64_t wid) {
    assert(g_cust_per_dist >= 1000);
    for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) {
        row_t * row;
        uint64_t row_id;
        t_customer->get_new_row(row, 0, row_id);
        row->set_primary_key(cid);

        row->set_value(C_ID, cid);
        row->set_value(C_D_ID, did);
        row->set_value(C_W_ID, wid);
        char c_last[LASTNAME_LEN];
        if (cid <= 1000)
            Lastname(cid - 1, c_last);
        else
            Lastname(NURand(255,0,999,wid-1), c_last);
        row->set_value(C_LAST, c_last);
#if !TPCC_SMALL
        char tmp[3] = "OE";
        row->set_value(C_MIDDLE, tmp);
        char c_first[FIRSTNAME_LEN];
        MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
        row->set_value(C_FIRST, c_first);
        char street[20];
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_CITY, street);
        char state[2];
        MakeAlphaString(2, 2, state, wid-1); /* State */
        row->set_value(C_STATE, state);
        char zip[9];
        MakeNumberString(9, 9, zip, wid-1); /* Zip */
        row->set_value(C_ZIP, zip);
        char phone[16];
        MakeNumberString(16, 16, phone, wid-1); /* Zip */
        row->set_value(C_PHONE, phone);
        row->set_value(C_SINCE, 0);
        row->set_value(C_CREDIT_LIM, 50000);
        row->set_value(C_DELIVERY_CNT, 0);
        char c_data[500];
        MakeAlphaString(300, 500, c_data, wid-1);
        row->set_value(C_DATA, c_data);
#endif
        if (RAND(10, wid-1) == 0) {
            char tmp[] = "GC";
            row->set_value(C_CREDIT, tmp);
        } else {
            char tmp[] = "BC";
            row->set_value(C_CREDIT, tmp);
        }
        row->set_value(C_DISCOUNT, (double)RAND(5000,wid-1) / 10000);
        row->set_value(C_BALANCE, -10.0);
        row->set_value(C_YTD_PAYMENT, 10.0);
        row->set_value(C_PAYMENT_CNT, 1);
        uint64_t key;
        key = custNPKey(c_last, did, wid);
        index_insert(i_customer_last, key, row, wh_to_part(wid));
        key = custKey(cid, did, wid);
        index_insert(i_customer_id, key, row, wh_to_part(wid));
    }
}


/**
 * The initialization of HISTORY table doesn't call index_insert(), because HISTORY table doesn't have an index.
 * Insert 1 tuple.
 */
void tpcc_wl::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
    row_t * row;
    uint64_t row_id;
    t_history->get_new_row(row, 0, row_id);
    row->set_primary_key(0);
    row->set_value(H_C_ID, c_id);
    row->set_value(H_C_D_ID, d_id);
    row->set_value(H_D_ID, d_id);
    row->set_value(H_C_W_ID, w_id);
    row->set_value(H_W_ID, w_id);
    row->set_value(H_DATE, 0);
    row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
    char h_data[24];
    MakeAlphaString(12, 24, h_data, w_id-1);
    row->set_value(H_DATA, h_data);
#endif

}


/**
 * The initialization of ORDER,ORDER_LINE,NEW_ORDER table doesn't call index_insert().
 * ORDER : Insert g_cust_per_dist(3000) tuples.
 * ORDER_LINE : Insert o_ol_cnt tuples.(o_ol_cnt is a random number in [5,15])
 * NEW_ORDER : Insert g_cust_per_dist-2100(900) tuples.
 */
void tpcc_wl::init_tab_order(uint64_t did, uint64_t wid) {
    uint64_t perm[g_cust_per_dist];
    init_permutation(perm, wid);            /* initialize permutation of customer numbers */
    for (uint64_t oid = 1; oid <= g_cust_per_dist; oid++) {
        row_t * row;
        uint64_t row_id;
        t_order->get_new_row(row, 0, row_id);
        uint64_t ord_primary = orderKey(oid, did, wid);
        row->set_primary_key(ord_primary);
        uint64_t o_ol_cnt = 1;
        uint64_t cid = perm[oid - 1]; //get_permutation();
        row->set_value(O_ID, oid);
        row->set_value(O_C_ID, cid);
        row->set_value(O_D_ID, did);
        row->set_value(O_W_ID, wid);
        uint64_t o_entry = 2013;
        row->set_value(O_ENTRY_D, o_entry);
        if (oid < 2101)
            row->set_value(O_CARRIER_ID, URand(1, 10, wid-1));
        else
            row->set_value(O_CARRIER_ID, 0);
        o_ol_cnt = URand(5, 15, wid-1);     // 5-15 order lines in each order
        row->set_value(O_OL_CNT, o_ol_cnt);
        row->set_value(O_ALL_LOCAL, 1);

        index_insert(i_order, ord_primary, row, wh_to_part(wid));
        // ORDER-LINE

        for (uint64_t ol = 1; ol <= o_ol_cnt; ol++) {
            t_orderline->get_new_row(row, 0, row_id);
            row->set_value(OL_O_ID, oid);
            row->set_value(OL_D_ID, did);
            row->set_value(OL_W_ID, wid);
            row->set_value(OL_NUMBER, ol);
            row->set_value(OL_I_ID, URand(1, 100000, wid-1));
#if !TPCC_SMALL
            row->set_value(OL_SUPPLY_W_ID, wid);
            if (oid < 2101) {
                row->set_value(OL_DELIVERY_D, o_entry);
                row->set_value(OL_AMOUNT, 0);
            } else {
                row->set_value(OL_DELIVERY_D, 0);
                row->set_value(OL_AMOUNT, (double)URand(1, 999999, wid-1)/100);
            }
            row->set_value(OL_QUANTITY, 5);
            char ol_dist_info[24];
            MakeAlphaString(24, 24, ol_dist_info, wid-1);
            row->set_value(OL_DIST_INFO, ol_dist_info);
#endif

            uint64_t ordline_primary = orderlineKey(ol, oid, did, wid);
            index_insert(i_orderline, ordline_primary, row, wh_to_part(wid));
        }

        // NEW ORDER
        if (oid > 2100) {
            t_neworder->get_new_row(row, 0, row_id);
            row->set_value(NO_O_ID, oid);
            row->set_value(NO_D_ID, did);
            row->set_value(NO_W_ID, wid);

            uint64_t ordnew_primary = neworderKey(oid, did, wid);
            index_insert(i_neworder, ordnew_primary, row, wh_to_part(wid));
        }
    }
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

/**
 * Initialize the cid of CUSTOMER table. [Guarantee the cid is not consecutive]
 * @param perm_c_id : array of customer_id.
 * @param wid : corresponding WAREHOUSE id.
 */
void tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
    uint32_t i;
    // Init with consecutive values
    for(i = 0; i < g_cust_per_dist; i++)
        perm_c_id[i] = i+1;

    // shuffle
    for(i=0; i < g_cust_per_dist-1; i++) {
        uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

/**
 * Every thread call this function to init the corresponding warehouse.
 * 1. Only thread 0 will init ITEM table.
 * 2.
 * @param This
 * @return
 */
void * tpcc_wl::threadInitWarehouse(void * This) {
    tpcc_wl * wl = (tpcc_wl *) This;
    int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
    uint32_t wid = tid + 1;
    tpcc_buffer[tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
    assert((uint64_t)tid < g_num_wh);
    srand48_r(wid, tpcc_buffer[tid]);

    if (tid == 0)
        wl->init_tab_item();

    wl->init_tab_wh( wid );
    wl->init_tab_dist( wid );
    wl->init_tab_stock( wid );
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
        wl->init_tab_cust(did, wid);
        wl->init_tab_order(did, wid);
        for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++)
            wl->init_tab_hist(cid, did, wid);
    }
    return NULL;
}


/**
 * Following functions are helping functions of IC3.
 */
#define ADD_SELF_EDGE(tpe, pid) \
  if (k == TPCC_ ## tpe) { \
    sc_graph[i][j][k].txn_type = TPCC_ ## tpe; \
    sc_graph[i][j][k].piece_id = pid; \
    cnt++; \
  }

#define ADD_EDGE(tpe, ptpe, pid) \
  else if (k == TPCC_ ## tpe && (g_perc_ ## ptpe > 0)) { \
    sc_graph[i][j][k].txn_type = TPCC_ ## tpe; \
    sc_graph[i][j][k].piece_id = pid; \
    cnt++; \
  }

#define ADD_ONLY_EDGE(tpe, ptpe, pid) \
  if (k == TPCC_ ## tpe && (g_perc_ ## ptpe > 0)) { \
    sc_graph[i][j][k].txn_type = TPCC_ ## tpe; \
    sc_graph[i][j][k].piece_id = pid; \
    cnt++; \
  }

#define ADD_OTHER_EDGE() \
  else \
    sc_graph[i][j][k].txn_type = TPCC_ALL;

#if CC_ALG == IC3
void
tpcc_wl::init_scgraph() {
    // XXX(zhihan): hard code sc-graph
    // for each pair of txn, there has a list of pieces
    // for each piece of each txn, there has a list of conflicting piece
    sc_graph = (SC_PIECE ***) _mm_malloc(sizeof(void *) * TPCC_ALL, 64);
    // percentage MUST MATCH order of TPCCTxnType in config file
    /*
    double percentage[TPCC_ALL] = {g_perc_payment, g_perc_neworder,
                                   g_perc_delivery, g_perc_orderstatus,
                                   g_perc_stocklevel};
    */
    int i, j, k, cnt;
    for (i = 0; i < TPCC_ALL; i++) {
        // for each txn
        if (i == TPCC_PAYMENT && (g_perc_payment > 0)) {
            sc_graph[i] = (SC_PIECE **) _mm_malloc(sizeof(void *) * 4, 64);
            // for each piece of txn type payment
            for (j = 0; j < IC3_TPCC_PAYMENT_PIECES; j++) {
                // has up to TPCC_ALL conflicting pieces
                sc_graph[i][j] = (SC_PIECE *) _mm_malloc(sizeof(SC_PIECE) * (TPCC_ALL
                                                                             + 1), 64); // add one dummy node at the end for metadata
                cnt = 0;
                if (j == 2) { // customer piece
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(PAYMENT, 2)
                        ADD_EDGE(DELIVERY, delivery, 3)
                            ADD_OTHER_EDGE()
                    }
#if !COMMUTATIVE_OPS
                } else if (j == 0) { // warehouse
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(PAYMENT, 0)
#if IC3_MODIFIED_TPCC
                            ADD_EDGE(NEW_ORDER, neworder, 0)
#endif
                            ADD_OTHER_EDGE()
                    }
                } else if (j == 1) { // district
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(PAYMENT, 1)
                        ADD_EDGE(NEW_ORDER, neworder, 1)
                            ADD_OTHER_EDGE()
                    }
#endif
                } else {
                    sc_graph[i][j][k].txn_type = TPCC_ALL;
                }
                sc_graph[i][j][TPCC_ALL].txn_type = TPCC_ALL;
                sc_graph[i][j][TPCC_ALL].piece_id = cnt;
            }
        } else if (i == TPCC_NEW_ORDER && (g_perc_neworder > 0)) {
            sc_graph[i] = (SC_PIECE **) _mm_malloc(sizeof(void *) * 8, 64);
            // for each piece of txn type payment
            for (j = 0; j < IC3_TPCC_NEW_ORDER_PIECES; j++) {
                // has up to TPCC_ALL conflicting pieces
                sc_graph[i][j] = (SC_PIECE *) _mm_malloc(sizeof(SC_PIECE) *
                                                         (TPCC_ALL + 1), 64);
                cnt = 0;
                if (j == 3) { // neworder piece
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(NEW_ORDER, 3)
                        ADD_EDGE(DELIVERY, delivery, 0)
                            ADD_OTHER_EDGE()
                    }
                } else if (j == 4) { // order piece
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(NEW_ORDER, 4)
                        ADD_EDGE(DELIVERY, delivery, 1)
                            ADD_OTHER_EDGE()
                    }
                } else if (j == 6) { // stock piece
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(NEW_ORDER, 6)
                            ADD_OTHER_EDGE()
                    }
                } else if (j == 7) { // order line piece
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(NEW_ORDER, 7)
                        ADD_EDGE(DELIVERY, delivery, 2)
                            ADD_OTHER_EDGE()
                    }
#if !COMMUTATIVE_OPS
#if IC3_MODIFIED_TPCC
                    } else if (j == 0) { // warehouse
          for (k = 0; k < TPCC_ALL; k++) {
            ADD_ONLY_EDGE(PAYMENT, payment, 0)
            ADD_OTHER_EDGE()
          }
#endif
                } else if (j == 1) { // district
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(NEW_ORDER, 1)
                            //ADD_EDGE(PAYMENT, payment, 1)
                            ADD_OTHER_EDGE()
                    }
#else
                    } else if (j == 1) { // district
          for (k = 0; k < TPCC_ALL; k++) {
              ADD_SELF_EDGE(NEW_ORDER, 1)
              ADD_OTHER_EDGE()
          }
#endif
                } else {
                    sc_graph[i][j][k].txn_type = TPCC_ALL;
                }
                sc_graph[i][j][TPCC_ALL].txn_type = TPCC_ALL;
                sc_graph[i][j][TPCC_ALL].piece_id = cnt;
            }
        } else if (i == TPCC_DELIVERY && (g_perc_delivery > 0)) {
            sc_graph[i] = (SC_PIECE **) _mm_malloc(sizeof(void *) * 4, 64);
            // for each piece of txn type delivery
            for (j = 0; j < IC3_TPCC_DELIVERY_PIECES; j++) {
                cnt = 0;
                // has up to TPCC_ALL conflicting pieces
                sc_graph[i][j] = (SC_PIECE *) _mm_malloc(sizeof(SC_PIECE) * (TPCC_ALL
                                                                             + 1), 64);
                if (j == 0) { // new order
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(DELIVERY, 0)
                        ADD_EDGE(NEW_ORDER, neworder, 3)
                            ADD_OTHER_EDGE()
                    }
                } else if (j == 1) { // order
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(DELIVERY, 1)
                        ADD_EDGE(NEW_ORDER, neworder, 4)
                            ADD_OTHER_EDGE()
                    }
                } else if (j == 2) { // order line
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(DELIVERY, 2)
                        ADD_EDGE(NEW_ORDER, neworder, 7)
                            ADD_OTHER_EDGE()
                    }
                } else if (j == 3) { // customer
                    for (k = 0; k < TPCC_ALL; k++) {
                        ADD_SELF_EDGE(DELIVERY, 3)
                        ADD_EDGE(PAYMENT, payment, 3)
                            ADD_OTHER_EDGE()
                    }
                } else {
                    sc_graph[i][j][k].txn_type = TPCC_ALL;
                }
                sc_graph[i][j][TPCC_ALL].txn_type = TPCC_ALL;
                sc_graph[i][j][TPCC_ALL].piece_id = cnt;
            }
        } else {
            sc_graph[i] = NULL;
        }
    }
}

SC_PIECE *
tpcc_wl::get_cedges(TPCCTxnType txn_type, int piece_id) {
    if (sc_graph == NULL)
        return NULL;
    if (sc_graph[txn_type] == NULL)
        return NULL;
    if (sc_graph[txn_type][piece_id][TPCC_ALL].piece_id == 0)
        return NULL;
    return sc_graph[txn_type][piece_id];
}
#endif

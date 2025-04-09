#include "txn.h"

#include "wl.h"
#include "ycsb.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "index_mbtree.h"
// for info of lock entry
#include "row_lock.h"
#include "row_bamboo.h"
#include "row_rr.h"
#include "row_mocc.h"
#include <unordered_map>

extern thread_local std::unordered_map<void*, uint64_t>  node_map;

void txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
    this->h_thd = h_thd;
    this->h_wl = h_wl;
    lock_ready = false;
    lock_abort = false;
    timestamp = 0;
#if CC_ALG == BAMBOO
    commit_barriers = 0;
#if BB_TRACK_DEPENDENS
    i_depend_set = new std::unordered_set<uint64_t>();
    dependend_on_me = new tbb::concurrent_unordered_set<uint64_t>();
#endif
#endif
    ready_part = 0;
    row_cnt = 0;
    wr_cnt = 0;
    insert_cnt = 0;
    remove_cnt = 0;
    insert_idx_cnt = 0;
    remove_idx_cnt = 0;
#if CC_ALG == MOCC
    cur_lock_list_head = 0;
    track_perf_sig = false;
    lock_rd_cnt = 0;
#endif

    // init accesses
    accesses = (Access **) _mm_malloc(sizeof(Access *) * MAX_ROW_PER_TXN, 64);
    for (int i = 0; i < MAX_ROW_PER_TXN; i++)
        accesses[i] = NULL;

#if CC_ALG == REBIRTH_RETIRE
    timestamp_v = 0;
    // Optimization for read_only long transaction.
    is_long = false;
    read_only = false;
#endif

    wait_latch_time = 0;
    wait_passive_retire = 0;
    num_accesses_alloc = 0;

#if CC_ALG == TICTOC || CC_ALG == SILO || CC_ALG == MOCC
    _pre_abort = (g_params["pre_abort"] == "true");
    if (g_params["validation_lock"] == "no-wait")
        _validation_no_wait = true;
    else if (g_params["validation_lock"] == "waiting")
        _validation_no_wait = false;
    else
        assert(false);
#endif
#if CC_ALG == TICTOC
    _max_wts = 0;
    _write_copy_ptr = (g_params["write_copy_form"] == "ptr");
    _atomic_timestamp = (g_params["atomic_timestamp"] == "true");
#elif CC_ALG == SILO || CC_ALG == MOCC
    _cur_tid = 0;
#elif CC_ALG == IC3
    depqueue = (TxnEntry **) _mm_malloc(sizeof(void *)*THREAD_CNT, 64);
  for (int i = 0; i < THREAD_CNT; i++)
    depqueue[i] = NULL;
  depqueue_sz = 0;
  piece_starttime = 0;
#endif
}

void txn_man::set_txn_id(txnid_t txn_id) {
#if CC_ALG == WOUND_WAIT || CC_ALG == BAMBOO || CC_ALG == REBIRTH_RETIRE || CC_ALG == DL_DETECT
    lock_abort = false;
    lock_ready = false;
    status = RUNNING;
#if CC_ALG == BAMBOO
    commit_barriers = 0;
    if (g_last_retire > 0)
        start_ts = get_sys_clock();
#endif
#endif
#if CC_ALG == IC3
    status = RUNNING;
    depqueue_sz = 0;
#endif
    this->txn_id = txn_id;
#if LATCH == LH_MCSLOCK
    mcs_node = new mcslock::mcs_node();
#endif
}

txnid_t txn_man::get_txn_id() {
    return this->txn_id;
}

workload * txn_man::get_wl() {
    return h_wl;
}

uint64_t txn_man::get_thd_id() {
    return h_thd->get_thd_id();
}

bool txn_man::atomic_set_ts(ts_t ts) {
    if (ATOM_CAS(timestamp, 0, ts)) {
        return true;
    }
    return false;
}

uint64_t txn_man::set_next_ts(int n) {
    if (atomic_set_ts(h_thd->get_next_n_ts(n))) {
        return this->timestamp;
    } else {
        return 0; // fail to set timestamp
    }
}
uint64_t txn_man::set_next_ts() {
    if (atomic_set_ts(h_thd->get_next_ts())) {
        return this->timestamp;
    } else {
        return 0; // fail to set timestamp
    }
}
void txn_man::reassign_ts() {
    this->timestamp = h_thd->get_next_n_ts(1);
}

void txn_man::set_ts(ts_t timestamp) {
    this->timestamp = timestamp;
}

ts_t txn_man::get_ts() {
    return this->timestamp;
}

void txn_man::cleanup(RC rc) {
    // go through accesses and release
    for (int rid = row_cnt - 1; rid >= 0; rid --) {
#if (CC_ALG == WOUND_WAIT) || (CC_ALG == BAMBOO) || (CC_ALG == REBIRTH_RETIRE)
        if (accesses[rid]->orig_row == NULL) {
            continue;
        }
#endif

        row_t * orig_r = accesses[rid]->orig_row;
        access_t type = accesses[rid]->type;

#if COMMUTATIVE_OPS
        if (accesses[rid]->com_op != COM_NONE && (rc != Abort)) {
                if (accesses[rid]->com_op == COM_INC)
                    orig_r->inc_value(accesses[rid]->com_col, accesses[rid]->com_val);
                else
                    orig_r->dec_value(accesses[rid]->com_col, accesses[rid]->com_val);
                accesses[rid]->com_op = COM_NONE;
            }
#endif

        if (type == WR && rc == Abort)
            type = XP;

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
        if (type == RD) {
            accesses[rid]->data = NULL;
            continue;
        }
#endif

#if COMMUTATIVE_OPS && !COMMUTATIVE_LATCH
        if (type != CM) {
#endif

#if CC_ALG == BAMBOO
        orig_r->return_row(accesses[rid]->lock_entry, rc);
        accesses[rid]->orig_row = NULL;
#elif CC_ALG == WOUND_WAIT
        orig_r->return_row(type, accesses[rid]->data, accesses[rid]->lock_entry);
        accesses[rid]->orig_row = NULL;
#elif CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
        if (ROLL_BACK && type == XP) {
        orig_r->return_row(type, this, accesses[rid]->orig_data);
    } else {
        orig_r->return_row(type,this, accesses[rid]->data);
    }
#elif CC_ALG == REBIRTH_RETIRE
        accesses[rid]->orig_row = NULL;
#else
        orig_r->return_row(type, this, accesses[rid]->data);
#endif

#if COMMUTATIVE_OPS && !COMMUTATIVE_LATCH
        }
#endif

#if CC_ALG != TICTOC && (CC_ALG != SILO) && (CC_ALG != WOUND_WAIT) && (CC_ALG!= BAMBOO) && (CC_ALG!= REBIRTH_RETIRE) && (CC_ALG != MOCC)
            // invalidate ptr for cc keeping globally visible ptr
            accesses[rid]->data = NULL;
#endif
    }

    if (rc == Abort ) {
        for (UInt32 i = 0; i < insert_cnt; i ++) {
            row_t * row = insert_rows[i];
            assert(g_part_alloc == false);
            row->is_deleted = 1;

            asm volatile ("sfence" ::: "memory");
#if CC_ALG != HSTORE && CC_ALG != OCC
            mem_allocator.free(row->manager, 0);
#endif
//            row->free_row();
//            mem_allocator.free(row, sizeof(row));
        }
    }


    row_cnt = 0;
    wr_cnt = 0;
    insert_cnt = 0;
    insert_cnt = 0;
    remove_cnt = 0;

#if CC_ALG == MOCC
    lock_rd_cnt = 0;
#endif

    insert_idx_cnt = 0;
    remove_idx_cnt = 0;
    node_map.clear();

#if CC_ALG == DL_DETECT
    dl_detector.clear_dep(get_txn_id());
#endif

#if CC_ALG == BAMBOO                // Make BamBoo support TEST workload
    commit_barriers = 0;
#endif
}

#if CC_ALG == BAMBOO || CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT || CC_ALG == REBIRTH_RETIRE
inline
void txn_man::assign_lock_entry(Access * access) {
#if CC_ALG == BAMBOO
#if PF_CS
    uint64_t starttime  = get_sys_clock();
#endif
    auto lock_entry = (BBLockEntry *) _mm_malloc(sizeof(BBLockEntry), 64);
    new (lock_entry) BBLockEntry(this, access);
#if PF_CS
    INC_STATS(this->get_thd_id(), time_creat_entry, get_sys_clock() - starttime);
#endif
#elif  CC_ALG == REBIRTH_RETIRE
#if PF_CS
    uint64_t starttime  = get_sys_clock();
#endif
    auto lock_entry = (RRLockEntry *) _mm_malloc(sizeof(RRLockEntry), 64);
    new (lock_entry) RRLockEntry(this, access);
#if PF_CS
    INC_STATS(this->get_thd_id(), time_creat_entry, get_sys_clock() - starttime);
#endif
#else
#if PF_CS
    uint64_t starttime  = get_sys_clock();
#endif
    auto lock_entry = (LockEntry *) _mm_malloc(sizeof(LockEntry), 64);
    new (lock_entry) LockEntry(this, access);
#if PF_CS
    INC_STATS(this->get_thd_id(), time_creat_entry, get_sys_clock() - starttime);
#endif
#endif
    access->lock_entry = lock_entry;
    //lock_entry->txn = this;
    //lock_entry->access = access;
}
#endif

/**
 * Record this operation in Accesses set and call corresponding get_row() to actually access the row
 * @param row : the row accesses by the txn [Full: with data]
 * @param type : access type
 * @return
 */
row_t * txn_man::get_row(row_t * row, access_t type) {
    if (CC_ALG == HSTORE)
        return row;

    uint64_t starttime = get_sys_clock();
    RC rc = RCOK;

    if (accesses[row_cnt] == NULL) {
        assert(row_cnt < MAX_ROW_PER_TXN);
        Access *access = (Access *) _mm_malloc(sizeof(Access), 64);

#if COMMUTATIVE_OPS
        // init
                access->com_op = COM_NONE;
#endif

        accesses[row_cnt] = access;

#if   (CC_ALG == SILO || CC_ALG == TICTOC || CC_ALG == MOCC)
        access->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
        access->data->init(MAX_TUPLE_SIZE);
        access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
        access->orig_data->init(MAX_TUPLE_SIZE);
#elif (CC_ALG == IC3)
        access->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
            access->data->init(MAX_TUPLE_SIZE);
            #if IC3_FIELD_LOCKING
                access->tids = (ts_t *) _mm_malloc(sizeof(ts_t) * MAX_FIELD_SIZE, 64);
            #else
                access->tid = 0;
            #endif
#elif (CC_ALG == WOUND_WAIT)
        // allocate lock entry as well
            assign_lock_entry(access);
#if PF_CS
        uint64_t starttime  = get_sys_clock();
#endif
            // for ww and bb, data is a local copy of original row for txn to work on
            access->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
            access->data->init(MAX_TUPLE_SIZE);
#if PF_CS
        INC_STATS(this->get_thd_id(), time_creat_entry, get_sys_clock() - starttime);
#endif
#elif (CC_ALG == BAMBOO)
        // allocate lock entry as well
            assign_lock_entry(access);
#if PF_CS
           uint64_t starttime  = get_sys_clock();
#endif
            // data is for making local changes before added to retired
            access->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
            access->data->init(MAX_TUPLE_SIZE);
            access->data->table = row->get_table();
            // orig data is for rollback
            access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
            access->orig_data->init(MAX_TUPLE_SIZE);
            access->orig_data->table = row->get_table();
#if PF_CS
        INC_STATS(this->get_thd_id(), time_creat_entry, get_sys_clock() - starttime);
#endif
#elif (CC_ALG == DL_DETECT || (CC_ALG == NO_WAIT) || (CC_ALG == WAIT_DIE))
        // allocate lock entry as well
            assign_lock_entry(access);
            access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
            access->orig_data->init(MAX_TUPLE_SIZE);
#elif CC_ALG == REBIRTH_RETIRE
        // allocate lock entry as well
        assign_lock_entry(access);
#endif
        num_accesses_alloc++;
    }

    if (row->is_deleted) {
        return NULL;
    }

#if (CC_ALG == WOUND_WAIT) || (CC_ALG == BAMBOO)
    rc = row->get_row(type, this, accesses[ row_cnt ]->orig_row, accesses[row_cnt]);
    if (rc == Abort) {
        accesses[row_cnt]->orig_row = NULL;
        return NULL;
    }
#elif CC_ALG == DL_DETECT || (CC_ALG == NO_WAIT) || (CC_ALG == WAIT_DIE)
    rc = row->get_row(type, this, accesses[ row_cnt ]->data, accesses[row_cnt]);
    if (rc == Abort){
        return NULL;
    }
    accesses[row_cnt]->orig_row = row;
#elif CC_ALG == IC3
    assert(rc == RCOK);
        // re-initialize read/write sets for the tuple.
        accesses[row_cnt]->rd_accesses = 0;
        accesses[row_cnt]->wr_accesses = 0;
        accesses[row_cnt]->lk_accesses = 0;
        accesses[row_cnt]->data->init_accesses(accesses[row_cnt]);
        accesses[row_cnt]->data->manager = row->manager;
        accesses[row_cnt]->data->table = row->get_table();
        accesses[row_cnt]->data->orig = row;
        accesses[row_cnt]->orig_row = row;
        #if !IC3_FIELD_LOCKING
            row->get_row(type, this, row, accesses[row_cnt]);
        #endif
#elif CC_ALG == REBIRTH_RETIRE                // Call get_row to actually access the row
    rc = row->get_row(type, this, accesses[row_cnt]->data, accesses[row_cnt]);
    accesses[row_cnt]->orig_row = row;

    if (rc == Abort) {
        accesses[row_cnt]->orig_row = NULL;
        return NULL;
    }
    auto temp_version = (Version*) accesses[row_cnt]->tuple_version;
    temp_version->data = row;
#else
    rc = row->get_row(type, this, accesses[ row_cnt ]->data, accesses[row_cnt]);
    if (rc == Abort) {
        return NULL;
    }
    accesses[row_cnt]->orig_row = row;
#endif

#if (CC_ALG == BAMBOO && BB_OPT_RAW)
    if (rc == FINISH) {
        // RAW optimization
        accesses[row_cnt]->data->table = row->get_table();
    }
#endif

    /**
     * Set the operation type of a certain access [row->get_row() successfully, so we can set access object directly]
     */
    accesses[row_cnt]->type = type;

#if CC_ALG == TICTOC
    accesses[row_cnt]->wts = last_wts;
    accesses[row_cnt]->rts = last_rts;
#elif CC_ALG == SILO || CC_ALG == MOCC
    accesses[row_cnt]->tid = last_tid;
#elif CC_ALG == HEKATON
    accesses[row_cnt]->history_entry = history_entry;
#endif

    if (row->is_deleted) { // safe: already deleted, lock is acquired but invalid.
        return NULL;
    }

    if (type == WR) {
#if CC_ALG == WOUND_WAIT
#if PF_CS
        uint64_t startt = get_sys_clock();
#endif
        // make local copy to work on
        accesses[row_cnt]->data->table = row->get_table();
        accesses[row_cnt]->data->copy(row);
#if PF_CS
        INC_STATS(get_thd_id(), time_copy, get_sys_clock() - startt);
#endif
#elif CC_ALG == BAMBOO
        #if PF_CS
        uint64_t startt = get_sys_clock();
        #endif
        // make local copy to work on
        accesses[row_cnt]->data->table = row->get_table();
        accesses[row_cnt]->data->copy(row);
        // make copy to rollback
        accesses[row_cnt]->orig_data->table = row->get_table();
        accesses[row_cnt]->orig_data->copy(row);
        #if PF_CS
        INC_STATS(get_thd_id(), time_copy, get_sys_clock() - startt);
        #endif
#elif CC_ALG == REBIRTH_RETIRE
#if PF_CS
        uint64_t startt = get_sys_clock();
#endif
        temp_version->data->copy(row);
#if PF_CS
        INC_STATS(get_thd_id(), time_copy, get_sys_clock() - startt);
#endif
#elif ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
        accesses[row_cnt]->orig_data->table = row->get_table();
            accesses[row_cnt]->orig_data->copy(row);
#endif
    }

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
    if (type == RD)
        row->return_row(type, accesses[ row_cnt ]->data, accesses[row_cnt]->lock_entry);
#endif

    row_cnt++;
    if (type == WR) {
        wr_cnt++;
    }

    uint64_t timespan = get_sys_clock() - starttime;
    INC_TMP_STATS(get_thd_id(), time_man, timespan);

#if  (CC_ALG == WOUND_WAIT)
    if (type == WR)
        return accesses[row_cnt - 1]->data;
    else
        return accesses[row_cnt - 1]->orig_row;
#elif CC_ALG == BAMBOO
    //printf("txn %lu got row %p at %d-th access %p\n", get_txn_id(), (void *)accesses[row_cnt - 1]->orig_row, row_cnt - 1, (void *)accesses[row_cnt - 1]);
        if (type == WR)
            return accesses[row_cnt - 1]->data;
        else {
            if (rc != FINISH)
              return accesses[row_cnt - 1]->orig_row;
            else
              return accesses[row_cnt - 1]->data; // RAW
        }
#elif CC_ALG == IC3
    return accesses[row_cnt - 1]->data;
#elif CC_ALG == REBIRTH_RETIRE
    auto res_version = (Version*) accesses[row_cnt - 1]->tuple_version;
    assert(res_version->data != nullptr);
    return res_version->data;
#else
    return accesses[row_cnt - 1]->data;
#endif
}
bool txn_man::remove_row(row_t* row) {
    remove_rows[remove_cnt++] = row;
    return true;
}
void txn_man::insert_row(row_t * row, table_t * table) {
    if (CC_ALG == HSTORE)
        return;
    assert(insert_cnt < MAX_ROW_PER_TXN);
    insert_rows[insert_cnt ++] = row;
}

//void txn_man::index_insert(row_t * row, INDEX * index, idx_key_t key) {
//    //TODO(zhihan): insert row in the index.
//    uint64_t part_id = get_part_id(row);
//    itemid_t * m_item = (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id);
//    m_item->init();
//    m_item->type = DT_row;
//    m_item->location = row;
//    m_item->valid = true;
//#ifdef NDEBUG
//    index->index_insert(key, m_item, part_id);
//#else
//    assert(index->index_insert(key, m_item, part_id) == RCOK);
//#endif
//}

RC txn_man::apply_index_changes(RC rc) {
#if WORKLOAD == TPCC
    if (rc == RCOK) rc = validate();
    if (rc != RCOK) {
        // Aborted, remove previously inserted placeholders.
        for (size_t i = 0; i < insert_idx_cnt; i++) {
            auto idx = insert_idx_idx[i];
            auto key = insert_idx_key[i];
            // auto row = insert_idx_row[i];
            auto part_id = insert_idx_part_id[i];
            auto rc_remove = idx->index_remove(key, part_id);
            // at this time, we still hold the lock of the inserted rows. cleanup will delete these rows.
//            assert(rc_remove == RCOK);
        }
        insert_idx_cnt = 0;
        return rc;
    }
    insert_idx_cnt = 0;
    for (size_t i = 0; i < remove_idx_cnt; i++) {
        auto idx = remove_idx_idx[i];
        auto key = remove_idx_key[i];
        auto part_id = remove_idx_part_id[i];
        // printf("remove_idx idx=%p key=%" PRIu64 " part_id=%d\n", idx, key, part_id);
        auto rc_remove = idx->index_remove(key, part_id);
//        assert(rc_remove == RCOK);
    }

    remove_idx_cnt = 0;

    // Free deleted rows
    for (size_t i = 0; i < remove_cnt; i++) {
        auto row = remove_rows[i];
//        assert(!row->is_deleted);
        row->is_deleted = 1;
    }
    remove_cnt = 0;

#endif

    return rc;
}

row_t* txn_man::search(index_base* index, uint64_t key, int part_id, access_t type) {
    itemid_t * item = NULL;
    item = index_read(index, key, part_id);
    if (item == NULL) {
        return NULL;
    }

    // printf("%lld, %lld\n", ((row_t *)item->location)->index_cnt, h_wl->get_index_cnt(index));
    assert(((row_t *)item->location)->index_cnt == h_wl->get_index_cnt(index));

    // cur_key = key;
    auto item_row = (row_t *)item->location;
    return get_row(item_row, type);
}
itemid_t * txn_man::index_read(index_base * index, idx_key_t key, int part_id) {
    // h_wl->update_index_accessed(index);
    uint64_t starttime = get_sys_clock();
    itemid_t * item = NULL;
    index->index_read(key, item, part_id, get_thd_id());
    INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
    return item;
}

void txn_man::index_read(index_base * index, idx_key_t key, int part_id, itemid_t *& item) {
    // h_wl->update_index_accessed(index);
    uint64_t starttime = get_sys_clock();
    index->index_read(key, item, part_id, get_thd_id());
    INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
}

RC txn_man::index_read_multiple(index_base* index, idx_key_t key, itemid_t** items, size_t& count, int part_id) {
    return index->index_read_multiple(key, items, count, part_id);
}

RC txn_man::index_read_range(index_base* index, idx_key_t min_key, idx_key_t max_key, itemid_t** items, size_t& count, int part_id) {
    return index->index_read_range(min_key, max_key, items, count, part_id);
}

RC txn_man::index_read_range_rev(index_base* index, idx_key_t min_key, idx_key_t max_key, itemid_t** items, size_t& count, int part_id) {
    return index->index_read_range_rev(min_key, max_key, items, count, part_id);
}

bool txn_man::insert_idx(index_base* index, uint64_t key, row_t* row, int part_id) {
    row->index_cnt = h_wl->get_index_cnt(index);

    itemid_t * m_item = (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id);
    m_item->init();
    m_item->type = DT_row;
    m_item->location = row;
    m_item->valid = true;

    auto rc_insert = index->index_insert(key, m_item, part_id); // May fail if others also insert one.

    if (rc_insert != RCOK) {
        return false;
    }

    assert(insert_idx_cnt < MAX_ROW_PER_TXN);

    insert_idx_idx[insert_idx_cnt] = index;
    insert_idx_key[insert_idx_cnt] = key;
    insert_idx_row[insert_idx_cnt] = row;
    insert_idx_part_id[insert_idx_cnt] = part_id;
    insert_idx_cnt++;
    // inserted += 1;
    // inserted_total += 1;
    return true;
}

bool txn_man::remove_idx(index_base* index, uint64_t key, row_t* row, int part_id) {
    (void)row;
    assert(remove_idx_cnt < MAX_ROW_PER_TXN);
    remove_idx_idx[remove_idx_cnt] = index;
    remove_idx_key[remove_idx_cnt] = key;
    remove_idx_part_id[remove_idx_cnt] = part_id;
    remove_idx_cnt++;
    return true;
}
//itemid_t * txn_man::index_read(INDEX * index, idx_key_t key, int part_id) {
//    uint64_t starttime = get_sys_clock();
//    itemid_t * item;
//    index->index_read(key, item, part_id, get_thd_id());
//    INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
//    return item;
//}
//
//void txn_man::index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item) {
//    uint64_t starttime = get_sys_clock();
//    index->index_read(key, item, part_id, get_thd_id());
//    INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
//}

RC txn_man::finish(RC rc) {
#if TPCC_USER_ABORT
    RC ret_rc = rc;
    if (rc == ERROR)
        rc = Abort;
#endif

#if THINKTIME > 0
    usleep(THINKTIME);
#endif

#if CC_ALG == HSTORE
    return RCOK;
#endif

#if CC_ALG == WOUND_WAIT || CC_ALG == BAMBOO || CC_ALG == REBIRTH_RETIRE
    for (int i = 0; i < wr_cnt; i ++) {
        if (accesses[i]->orig_row->is_deleted){
            rc = Abort;
        }
    }
#endif

    uint64_t starttime = get_sys_clock();
#if CC_ALG == OCC
    if (rc == RCOK)
        rc = occ_man.validate(this);
    else
        cleanup(rc);
#elif CC_ALG == TICTOC
    if (rc == RCOK)
		rc = validate_tictoc();  // tictoc,silo,mocc all apply_index_changes in validate processing
	else {
		rc = apply_index_changes(rc);
		cleanup(rc);
	}
#elif CC_ALG == SILO
    if (rc == RCOK){
        rc = validate_silo();
    }
	else {
	    rc = apply_index_changes(rc);
	    cleanup(rc);
	}
#elif CC_ALG == MOCC
    if (rc == RCOK) {
		rc = validate_mocc();
	} else {
		rc = apply_index_changes(rc);
		cleanup(rc);
	}
#elif CC_ALG == IC3
    if (rc == RCOK) {
    rc = validate_ic3();
    if (rc == RCOK) {
      if (!ATOM_CAS(status, RUNNING, COMMITED))
        rc = Abort;
    } else {
      status = ABORTED;
    }
  } else { // abort an txn
    // involve cascading aborts
    status = ABORTED; // may overwritten the aborts set by others.
  }
  if (rc == Abort)
    abort_ic3();
  cleanup(rc);
#elif CC_ALG == HEKATON
    rc = validate_hekaton(rc);
	cleanup(rc);
#elif CC_ALG == REBIRTH_RETIRE
//#if  WAIT_RR
//
//        for (int rid = row_cnt - 1; rid > retire_threshold; rid--) {
//            if (accesses[rid]->lock_entry->type == LOCK_SH)
//                continue;
//            accesses[rid]->orig_row->retire_row(accesses[rid]->lock_entry);
//        }
//
//#endif

    rc = validate_rr(rc);
    rc = apply_index_changes(rc);
    cleanup(rc);
#elif CC_ALG == WOUND_WAIT
    if (rc == RCOK) {
        if (!ATOM_CAS(status, RUNNING, COMMITED))
            rc = Abort;
	}
	rc = apply_index_changes(rc);
	cleanup(rc);
#elif CC_ALG == BAMBOO
  if (rc == Abort)
      status = ABORTED;
  else {
    uint64_t starttime = get_sys_clock();
    while (!ATOM_CAS(commit_barriers, 0, COMMITED)) {
        if (commit_barriers & ABORTED) {
            rc = Abort;
            break;
        }
        if (g_last_retire > 0 && (retire_threshold < row_cnt - 1)) {
                uint64_t lapse = get_sys_clock();
                if ((lapse - starttime) >= (lapse - start_ts) * g_last_retire) {
                    for (int rid = row_cnt - 1; rid > retire_threshold; rid--) {
                        if (accesses[rid]->lock_entry->type == LOCK_SH)
                            continue;
                        accesses[rid]->orig_row->retire_row(accesses[rid]->lock_entry);
                    }
                    retire_threshold = row_cnt - 1;
                }
        }
    }
#if PF_BASIC
    uint64_t timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), time_commit, timespan);
    this->wait_latch_time = this->wait_latch_time + timespan;
#endif
  }

  rc = apply_index_changes(rc);
  cleanup(rc);
#else
  rc = apply_index_changes(rc); // if abort, remove row from the index, flag the row as deleted
  cleanup(rc);
#endif

    uint64_t timespan = get_sys_clock() - starttime;
    INC_TMP_STATS(get_thd_id(), time_man,  timespan);
    INC_STATS(get_thd_id(), time_cleanup,  timespan);

#if TPCC_USER_ABORT
    if (rc == Abort && (ret_rc == ERROR)) {
        return ret_rc;
    }
#endif
    return rc;
}

void txn_man::release() {
    for (int i = 0; i < num_accesses_alloc; i++) {
    #if CC_ALG == BAMOO || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == DL_DETEC
        delete accesses[i]->lock_entry;
    #endif
        mem_allocator.free(accesses[i], 0);
    }
    mem_allocator.free(accesses, 0);
#if LATCH == LH_MCSLOCK
    delete mcs_node;
#endif
}

RC txn_man::validate() {
    for (auto it : node_map) {
        if (IndexMBTree::extract_version(it.first) != it.second) {
            return Abort;
        }
    }
    return RCOK;
}

#if COMMUTATIVE_OPS
void txn_man::inc_value(int col, uint64_t val) {
  // store operation and execute at commit time
  Access * access = accesses[row_cnt-1];
  access->com_op = COM_INC;
  access->com_val = val;
  access->com_col = col;
}

void txn_man::dec_value(int col, uint64_t val) {
  // store operation and execute at commit time
  Access * access = accesses[row_cnt-1];
  access->com_op = COM_DEC;
  access->com_val = val;
  access->com_col = col;
}
#endif


#if CC_ALG == MOCC
bool
txn_man::is_locked(uint64_t key) {
    for (int i = 0; i < cur_lock_list_head; ++i) {
        if (cur_lock_list[i].row->get_primary_key() == key && cur_lock_list[i].state)
            return true;
    }
    return false;
}

void
txn_man::remove_non_cononical_lock(uint64_t key) {
    for (int i = (cur_lock_list_head - 1); i >= 0; --i) {
        if (!cur_lock_list[i].state)
            continue;

        if (cur_lock_list[i].row->get_primary_key() < key) {
            cur_lock_list[i].row->manager->unlock(this, cur_lock_list[i].lt);
        } else {
            cur_lock_list_head = i + 1;
            return;
        }
    }

    cur_lock_list_head = 0;
}

void
txn_man::insert_cononical_lock(int lt, row_t *row) {
    cur_lock_list[cur_lock_list_head].row = row;
    cur_lock_list[cur_lock_list_head].lt = lt;
    cur_lock_list[cur_lock_list_head].state = true;
    cur_lock_list_head += 1;
}

void
txn_man::remove_cononical_lock(uint64_t key) {
    for (int i = 0; i < cur_lock_list_head; ++i) {
        if (cur_lock_list[i].row->get_primary_key() == key)
            cur_lock_list[i].state = false;
    }
}

void
txn_man::unlock_read_locks_all() {
    for (int i = 0; i < cur_lock_list_head; ++i) {
        if (cur_lock_list[i].lt == LOCK_SH && cur_lock_list[i].state)
            cur_lock_list[i].row->manager->unlock(this, LOCK_SH);
    }
}

void
txn_man::clear_lock_state(RC rc) {
    for (int i = 0; i < cur_lock_list_head; ++i) {
        if (cur_lock_list[i].lt == LOCK_SH && cur_lock_list[i].state)
            cur_lock_list[i].row->manager->unlock(this, LOCK_SH);
        if (cur_lock_list[i].lt == LOCK_EX && cur_lock_list[i].state) {
            cur_lock_list[i].row->manager->unlock(this, LOCK_EX);
        }
    }
    cur_lock_list_head = 0;
}

#endif

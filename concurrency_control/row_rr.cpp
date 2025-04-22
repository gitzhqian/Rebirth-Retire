//
// Created by root on 2024/7/18.
//

#include "manager.h"
#include "row_rr.h"
#include "mem_alloc.h"
#include <mm_malloc.h>
#include "thread.h"

#if CC_ALG == REBIRTH_RETIRE

void Row_rr::init(row_t *row){
    // initialize version header
    version_header = (Version *) _mm_malloc(sizeof(Version), 64);

    version_header->begin_ts = 0;
    version_header->end_ts = INF;
    version_header->type = XP;
    version_header->read_queue = NULL;

    version_header->data = row;

    // pointer must be initialized
    version_header->prev = NULL;
    version_header->next = NULL;
    version_header->retire = NULL;

#if PREFETCH
    prefh_len = 16;
    prefh_latest = 0;
    version_prefhs_ = (Version **) _mm_malloc(sizeof(Version *) * prefh_len, 64);
    for (uint32_t i = 0; i < prefh_len; i++){
        version_prefhs_[i] = NULL;
    }
    version_prefhs_[0] = version_header;

#endif

#if LATCH == LH_SPINLOCK
    spinlock_row = new pthread_spinlock_t;
    pthread_spin_init(spinlock_row, PTHREAD_PROCESS_SHARED);
#else
    latch_row = new mcslock();
#endif

    latest = version_header;
    owner = nullptr;
    wait_list = new std::list<RRLockEntry *>();
    chain_threshold = 500;
}

RC Row_rr::read_committed(txn_man * txn, Version* latest_committed_, uint32_t prefh_latest_,
                          uint64_t start_r_w, Access * access){
    uint64_t startt_r_w = start_r_w;
    Version *latest_committed = latest_committed_;
    uint32_t idx = prefh_latest_;
#if PREFETCH
    __builtin_prefetch(reinterpret_cast<void *>(version_prefhs_[idx]), 0, 3);
#endif

    // assign timestamp using allocate thread local
    ts_t ts = txn->get_ts();
    ts = assign_ts(ts, txn);

    // read history version, use prefetching
    uint32_t num = 0;
    while (true) {
        if (latest_committed == nullptr) {
            txn->lock_abort = true;
            return Abort;
        }
#if PREFETCH
        idx = (idx == 0)? 0 : idx - 1;
        __builtin_prefetch(reinterpret_cast<void *>(version_prefhs_[idx]), 0, 3);
#endif

        if (num > chain_threshold) {
            if (latest_committed->type == AT || latest_committed->begin_ts == INF) {
                latest_committed = latest_committed->next;
                continue;
            }
            break;
        }
        if (latest_committed->type == XP && ts >= latest_committed->begin_ts) {
            assert(latest_committed->retire == nullptr);
            break;
        }

        latest_committed = latest_committed->next;
        num ++ ;
    }

    assert(latest_committed != nullptr);
    assert(latest_committed->data != nullptr);

    auto max_ts = latest_committed->begin_ts;
    if (max_ts > ts){
        txn->set_ts(max_ts);
    }

    assert(max_ts != INF);
    txn->lock_ready = true;
    access->tuple_version = latest_committed;

#if PF_CS
    INC_STATS(txn->get_thd_id(), time_read_write,  (get_sys_clock() - startt_r_w));
    txn->wait_latch_time = txn->wait_latch_time + (get_sys_clock() - startt_r_w);
#endif

    return RCOK;

}

Version *create_new_version(txn_man * txn){
    Version* new_version = nullptr;
#if PF_CS
    uint64_t starttime_creat = get_sys_clock();
#endif
    auto reserve_version = txn->h_thd->reserve_version();
    if (reserve_version == nullptr){
        new_version = (Version *) _mm_malloc(sizeof(Version), 64);
        new_version->init();
        new_version->next = nullptr;
        new_version->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
        new_version->data->init(g_max_tuple_size);
    } else{
        new_version = reserve_version;
        new_version->init();
        new_version->next = nullptr;
    }
#if PF_CS
    uint64_t endtime_creat = get_sys_clock();
    INC_STATS(txn->get_thd_id(), time_creat_version, endtime_creat - starttime_creat);
#endif

    return new_version;
}

RC Row_rr::access(txn_man * txn, TsType type, Access * access){
    // for long transaction, assign timestamp at first read:
    //     first read init the ts, dynamic adjust by following read max{read.beginTs}
    uint64_t startt_r_w = get_sys_clock();
    if(txn->is_long ){
        lock_row(txn);
        COMPILER_BARRIER
        Version * latest_committed = latest;
        uint32_t idx = prefh_latest;
        unlock_row(txn);
        COMPILER_BARRIER

        auto ret = read_committed(txn, latest_committed, idx, startt_r_w, access);

        return ret;
    }

    // for read-after-write, creat version, pre allocate memory space
    Version* new_version = nullptr;
    if (type == P_REQ) {
        new_version = create_new_version(txn);
    }

    RC rc = RCOK;
#if PF_CS
    uint64_t startt_get_latch = get_sys_clock();
#endif
    RRLockEntry * entry = get_entry(access);

    if (txn->status == ABORTED) {
        rc = Abort;
        txn->lock_abort = true;
        txn->lock_ready = false;
        return rc;
    }

    lock_row(txn);
    COMPILER_BARRIER
#if PF_CS
    uint64_t end_get_latch = get_sys_clock();
    uint64_t timespan1 = end_get_latch - startt_get_latch;
    INC_STATS(txn->get_thd_id(), time_get_latch,  timespan1);
    txn->wait_latch_time = txn->wait_latch_time + timespan1;
    startt_get_latch = end_get_latch;
#endif

    if (txn->status == ABORTED){
        rc = Abort;
        txn->lock_abort = true;
        txn->lock_ready = false;
#if PF_CS
        uint64_t timespan2 = get_sys_clock() - startt_get_latch;
        INC_STATS(txn->get_thd_id(), time_get_cs, timespan2);
        txn->wait_latch_time = txn->wait_latch_time + timespan2;
#endif
        unlock_row(txn);
        return rc;
    }

    // clear old versions
    remove_tombstones();

    // start read, write
    ts_t ts = txn->get_ts();
    if (type == R_REQ) {
        txn_man *retire_txn = version_header->retire;
        Version *read_version = version_header;
#if WAIT_RR
        if (ts == 0) {
            if (owner) {
                auto own_txn = owner->txn;
                assign_ts(own_txn->get_ts(), own_txn);
                ts = assign_ts(ts, txn);
            }
            ts = assign_ts(ts, txn);
        }

        if (owner) {
            if (a_higher_than_b(owner->txn->get_ts(), ts)) {
                rc = WAIT;
            } else {
                access->tuple_version = latest;
                goto final;
            }
        }else{
            access->tuple_version = latest;
            bring_next(txn, txn );
            goto final;
        }
#else
        if (owner) {
            auto own_txn = owner->txn;
            auto own_ts = own_txn->get_ts();
            own_ts = assign_ts(own_ts, own_txn);
            ts = assign_ts(ts, txn);
            if (a_higher_than_b(own_ts, ts)) {
                rc = WAIT;
            } else {
                read_version = owner->access->tuple_version;
            }
        } else {
            if (version_header->type == XP || (version_header->retire!= nullptr && version_header->retire->status == validating)){
                access->tuple_version = read_version;
                goto final;
            }else {
                ts = assign_ts(ts, txn);
                while (true) {
                    if (read_version == nullptr){
                        rc = Abort;
                        goto final;
                    }
                    if (read_version->type == XP){
                        if (read_version->begin_ts < ts){
                            break;
                        }
                    }
                    if (read_version->type == WR){
                        if (read_version->retire->get_ts() < ts){
                            break;
                        }
                    }

                    read_version = read_version->next;
                }
            }
        }
#endif

        if (rc == WAIT){
            assert(ts > 0);
            entry->type = LOCK_SH;
            entry->status = LOCK_WAITER;
            add_to_waiters(ts, entry);
        } else {
            retire_txn = read_version->retire;
            auto thd_id = txn->get_thd_id();
            // read a committed
            if (retire_txn == nullptr) {
                access->tuple_version = read_version;
                goto final;
            } else {
                // read a uncommitted, tracking the dependency
#if CHILDOPT
                retire_txn->children_bitmap[thd_id]= 1;
#else
                auto mk = std::make_pair(txn, DepType::WRITE_READ_);
                retire_txn->children.push_back(mk);
#endif
                retire_txn->timestamp_v.fetch_add(1,memory_order_relaxed );
                auto mk_p = std::make_pair(retire_txn, DepType::WRITE_READ_);
                txn->parents.push_back(mk_p);

                // add the read to the read queue
                auto hreader = new HReader(txn);
                auto curr_hreader = read_version->read_queue;
                if (curr_hreader == nullptr){
                    hreader->next = nullptr;
                    hreader->prev = read_version;
                    read_version->read_queue = hreader;
                } else {
                    hreader->prev = read_version;
                    hreader->next = read_version->read_queue;
                    read_version->read_queue = hreader;
                }
            }
        }

        access->tuple_version = read_version;
    }else if (type == P_REQ) {
#if WAIT_RR
        if (txn->get_ts() == 0) {
            if (owner) {
                assign_ts(owner->txn->get_ts(), owner->txn);
                ts = assign_ts(ts, txn);
            }else {
                ts = assign_ts(ts, txn);
                if (waiter_cnt <= 0) {
                    access->old_version = version_header;
                    new_version->next = version_header;
                    new_version->retire = txn;
                    new_version->type = WR;
                    version_header->prev = new_version;
                    version_header = new_version;
                    assert(version_header->end_ts == INF);

                    access->tuple_version = new_version;
                    entry->type = LOCK_EX;
                    entry->has_write = true;
                    entry->status = LOCK_OWNER;
                    entry->access = access;
                    entry->txn = txn;
                    owner = entry;

                    rc = RCOK;
                    goto final;
                }
            }
        }
#else
        // no conflict , grab the time, become the owner
        if ((owner == nullptr && (version_header->type == XP || (version_header->retire!= nullptr && version_header->retire->status == validating))) ||
                (owner != nullptr && owner->txn->status == validating)) {
            access->old_version = version_header;
            new_version->next = version_header;
            new_version->retire = txn;
            new_version->type = WR;
            version_header->prev = new_version;
            version_header = new_version;
            assert(version_header->end_ts == INF);

            access->tuple_version = new_version;
            entry->type = LOCK_EX;
            entry->has_write = true;
            entry->status = LOCK_OWNER;
            entry->access = access;
            entry->txn = txn;
            owner = entry;

            rc = RCOK;
            goto final;
        }

        // has conflict, assign timestamp
        ts = txn->get_ts();

        if (ts == 0) {
            std::vector<txn_man *> assign_txns;
            if (owner) {
                if (owner->txn->get_ts() == 0){
                    assign_txns.push_back(owner->txn);
                }
            }

            Version *assg_retire = version_header;
            while (true) {
                if (assg_retire->type == XP) break;
                if (assg_retire->type == AT) {
                    assg_retire = assg_retire->next;
                    continue;
                }
                if (assg_retire->retire != nullptr) {
                    auto ts_retire = assg_retire->retire->get_ts();
                    if (ts_retire == 0){
                        assign_txns.push_back(assg_retire->retire);
                    }
                }

                assg_retire = assg_retire->next;
            }

            for (int i = assign_txns.size() - 1; i >= 0; --i) {
                auto assign_txn = assign_txns[i];
                assign_ts(assign_txn->get_ts(), assign_txn);
            }

            ts = assign_ts(ts, txn);
        }
#endif

        // detect conflicts, if need to wound or need to rebirth
        wound_rebirth(ts, txn, type);
        if (txn->status == ABORTED) {
            rc = Abort;
            goto final;
        }

        new_version->retire = txn;
        new_version->type = WR;
        access->tuple_version = new_version;

        assert(txn->get_ts() > 0);
        rc = WAIT;
        txn->lock_ready = false;
        entry->type = LOCK_EX;
        entry->has_write = false;
        entry->status = LOCK_WAITER;
        entry->access = access;
        entry->txn = txn;
        add_to_waiters(ts, entry);
    }

    if (txn->lock_abort || txn->status == ABORTED) {
        rc = Abort;
        txn->lock_abort = true;
        txn->lock_ready = false;
    } else {
        if (rc == RCOK){
            assert(rc == RCOK);
            rc = RCOK;
        } else {
            assert(rc == WAIT);
            rc = WAIT;
            txn->lock_abort = false;
            txn->lock_ready = false;
        }
    }

#if PF_CS
    INC_STATS(txn->get_thd_id(), time_get_cs, (get_sys_clock() - startt_get_latch));
    txn->wait_latch_time = txn->wait_latch_time + (get_sys_clock() - startt_get_latch);
#endif

    //bring next waiter
    if (bring_next(txn, txn )) {
        rc = RCOK;
    }

final:
    unlock_row(txn);
    COMPILER_BARRIER

    if (rc == RCOK){
        txn->lock_abort = false;
        txn->lock_ready = true;
    }

    return  rc;
}

bool Row_rr::bring_next(txn_man *txn, txn_man *curr) {
    bool has_txn = false;
#if PF_CS
    uint64_t timestart_passive = get_sys_clock();
#endif
    // remove the aborted txn, GC
    remove_tombstones();

#if PASSIVE_RETIRE
    // passive retire the owner
    if (owner != nullptr){
        while (!owner){
            if (owner->has_write){
                break;
            }
            if (owner->txn != nullptr && owner->txn->status == ABORTED){
                break;
            }

            PAUSE
        }

        if (owner && owner->status == LOCK_OWNER && owner->txn != nullptr){
            // move it out of the owner
            owner->status = LOCK_RETIRED;
            // passive retire the owner, move the owner to the retire tail
            auto access_version = owner->access->tuple_version;
            if (access_version != nullptr){
                if (access_version != version_header->prev){
                    if (access_version->type != AT){
                        version_header->prev = access_version;
                        version_header = access_version;   // become the version header
                    }
                }
            }
        }

    #if PF_CS
        // the timespan of passive-retire wait, add it to the time_wait
        uint64_t timeend_passive = get_sys_clock();
        uint64_t timespan = timeend_passive - timestart_passive;
        INC_TMP_STATS(curr->get_thd_id(), time_wait, timespan);
        curr->wait_passive_retire = curr->wait_passive_retire + timespan;
    #endif

        owner = nullptr;
    }
#endif

    uint64_t retire_cs_ = get_sys_clock();
    // if any waiter can join the owners, just do it!
    for (auto it = wait_list->begin(); it != wait_list->end(); ++it) {
        auto entry = *it;
        if (entry->access == NULL || entry->txn == NULL || entry->txn->lock_abort) {
            continue;
        }

        if (!owner) {
            if (entry->type == LOCK_EX) {
                // will be reclaimed in the GC processing
                if (entry->status != LOCK_WAITER) continue;
                // promote a waiter to become the owner
                owner = entry;
                owner->status = LOCK_OWNER;

                // Check if the owner has a valid access after assignment
                if (owner->access == nullptr) {
                    owner = nullptr;
                    continue;
                }

#if WAIT_RR
                has_txn = bring_out_waiter(entry, txn);

                owner->access->old_version = version_header;
                owner->access->tuple_version->next = version_header;
                version_header->prev = owner->access->tuple_version;
                version_header = owner->access->tuple_version;
                entry->txn->lock_ready = true;

                break;
#endif

                // add owner depended on the retired tail
                auto retire_tail = version_header->retire;
                auto readers = version_header->read_queue;
                bool has_depend = false;
                if (retire_tail != nullptr && retire_tail->status == RUNNING) {
                    if (readers != nullptr) {
                        HReader *dep_read_ = readers;
                        while (true) {
                            if (dep_read_ == nullptr) break;
                            auto dep_read_txn_ = dep_read_->cur_reader;
                            if (dep_read_txn_ != nullptr && dep_read_txn_->status == RUNNING) {
                                if (dep_read_txn_->get_thd_id() != owner->txn->get_thd_id()) {
#if CHILDOPT
                                    auto thd_id = owner->txn->get_thd_id();
                                    dep_read_txn_->children_bitmap[thd_id]= 1;
#else
                                    auto mk = std::make_pair(owner->txn, DepType::READ_WRITE_);
                                    dep_read_txn_->children.push_back(mk);
#endif
                                    dep_read_txn_->timestamp_v.fetch_add(1 ,memory_order_relaxed);
                                    auto mk_p = std::make_pair(dep_read_txn_, DepType::READ_WRITE_);
                                    if (owner->txn != nullptr) {
                                        owner->txn->parents.push_back(mk_p);
                                        has_depend = true;
                                    }
                                }

                                break;
                            }

                            dep_read_ = dep_read_->next;
                        }
                    }

                    if (!has_depend && retire_tail != nullptr && retire_tail->status == RUNNING) {
#if CHILDOPT
                        auto thd_id = owner->txn->get_thd_id();
                        retire_tail->children_bitmap[thd_id] = 1;
#else
                        auto mk = std::make_pair(owner->txn, DepType::WRITE_WRITE_);
                        retire_tail->children.push_back(mk);
#endif
                        retire_tail->timestamp_v.fetch_add(1,memory_order_relaxed );
                        auto mk_p = std::make_pair(retire_tail, DepType::WRITE_WRITE_);
                        auto& owner_parents = owner->txn->parents;
                        owner_parents.push_back(mk_p);
                    }
                }

                // Ensure owner->access is not nullptr before accessing it
                if (owner->access != nullptr) {
                    owner->access->old_version = version_header;
                    owner->access->tuple_version->next = version_header;
                } else {
                    owner = nullptr;
                }

                has_txn = bring_out_waiter(entry, txn);
                entry->txn->lock_ready = true;
                if (entry->status != LOCK_OWNER) {
                    owner = nullptr;
                }
                if (owner == nullptr) {
                    continue;
                }

                break;
            } else {
                // may promote multiple readers
                if (entry->access == nullptr || entry->txn == nullptr || entry->txn->lock_abort) {
                    has_txn = false;
                } else {
#if WAIT_RR
                    has_txn = bring_out_waiter(entry, txn);
                    entry->txn->lock_ready = true;
                    entry->access->tuple_version = version_header;
#else
                    has_txn = bring_out_waiter(entry, txn);

                    Version *read_version;
                    if (owner){
                        read_version = owner->access->tuple_version;
                    } else {
                        read_version = version_header;
                    }
                    auto retire_txn = read_version->retire;
                    auto en_txn = entry->txn;
                    if (retire_txn != nullptr && retire_txn->status == RUNNING){
                        auto hreader = new HReader(en_txn);
                        auto curr_hreader = read_version->read_queue;
                        if (curr_hreader == nullptr){
                            hreader->prev = read_version;
                            read_version->read_queue = hreader;
                        } else {
                            hreader->prev = read_version;
                            hreader->next = read_version->read_queue;
                            read_version->read_queue = hreader;
                        }
#if CHILDOPT
                        auto thd_id = en_txn->get_thd_id();
                        retire_txn->children_bitmap[thd_id]= 1;
#else
                        auto mk = std::make_pair(en_txn,  DepType::WRITE_READ_);
                        retire_txn->children.push_back(mk);
#endif
                        retire_txn->timestamp_v.fetch_add(1 ,memory_order_relaxed);
                        auto mk_p = std::make_pair(retire_txn, DepType::WRITE_READ_);
                        en_txn->parents.push_back(mk_p);
                    }

                    en_txn->lock_ready = true;
                    entry->access->tuple_version = read_version;
                    assert(read_version != nullptr);
#endif
                }
            }
        } else {
            break;
        }
    }

#if PF_CS
    INC_STATS(curr->get_thd_id(), time_retire_cs, (get_sys_clock() - retire_cs_));
    curr->wait_latch_time = curr->wait_latch_time + (get_sys_clock() - retire_cs_);
#endif

    return has_txn;
}

RC Row_rr::active_retire(RRLockEntry * entry ) {
    RC rc = RCOK;

    if (entry->txn == nullptr){
        return rc;
    }
    uint64_t startt_retire = get_sys_clock();
    lock_row(entry->txn);
    COMPILER_BARRIER
#if PF_CS
    uint64_t end_retire = get_sys_clock();
    uint64_t  timespan = end_retire - startt_retire;
    INC_STATS(entry->txn->get_thd_id(), time_retire_latch,  timespan);
    entry->txn->wait_latch_time = entry->txn->wait_latch_time + timespan;
    startt_retire = end_retire;
#endif

    if (entry->type == LOCK_EX) {
        // remove the aborted txn, GC
        if (entry->status == LOCK_OWNER && entry->txn != nullptr && entry->txn->status != ABORTED) {
            if (owner != nullptr && owner->access != nullptr) {
                // there exist someone who is owner but need not retire truely
                if (owner->txn->get_thd_id() == entry->txn->get_thd_id()) {
                    // move it out of the owner
                    entry->status = LOCK_RETIRED;
                    // active retire the owner, move the owner to the retire tail
                    version_header->prev = entry->access->tuple_version;
                    version_header = entry->access->tuple_version;

                    owner = nullptr;
                }
            }
        } else if ( entry->status == LOCK_DROPPED || entry->status == LOCK_OWNER || entry->status == LOCK_RETIRED){
            if (entry->txn != nullptr && entry->txn->status == ABORTED) {
                if(owner == entry) {
                    owner = nullptr;
                }
                rc = Abort;
            }
        } else {
            rc = Abort;
        }
    }

    if (!owner){
        auto ret = bring_next(nullptr, entry->txn);
        if (ret){
            rc = Abort;
        }
    }

#if PF_CS
    uint64_t timespan1 = get_sys_clock() - startt_retire;
    INC_STATS(entry->txn->get_thd_id(), time_retire_cs, timespan1);
    entry->txn->wait_latch_time = entry->txn->wait_latch_time + timespan1;
#endif

    unlock_row(entry->txn);
    COMPILER_BARRIER

    return rc;
}

#endif



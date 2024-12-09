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

//    version_header->dynamic_txn_ts = (volatile ts_t *)_mm_malloc(sizeof(ts_t), 64);
//    version_header->dynamic_txn_ts = new ts_t(0);
    version_header->type = XP;
    version_header->read_queue = NULL;

    // pointer must be initialized
    version_header->prev = NULL;
    version_header->next = NULL;
    version_header->retire = NULL;

//    blatch = false;
#if LATCH == LH_SPINLOCK
    spinlock_row = new pthread_spinlock_t;
    pthread_spin_init(spinlock_row, PTHREAD_PROCESS_SHARED);
#else
    latch_row = new mcslock();
#endif

    owner = nullptr;
    entry_list = new std::list<RRLockEntry *>();
}


RC Row_rr::access(txn_man * txn, TsType type, Access * access){

    // Optimization for read_only long transaction.
#if READ_ONLY_OPTIMIZATION_ENABLE
    if(txn->is_long && txn->read_only){
        while(!ATOM_CAS(blatch, false, true)){
            PAUSE
        }

        Version* read_only_version = version_header;
        while (read_only_version){
            if(read_only_version->begin_ts != UINT64_MAX){
                assert(read_only_version->retire == NULL);
                access->tuple_version = read_only_version;
                break;
            }
            read_only_version = read_only_version->next;
        }

        blatch = false;
        return RCOK;
    }
#endif

    Version* new_version = nullptr;
    if (type == P_REQ) {
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
    }

    RC rc = RCOK;
    if (txn->status == ABORTED){
        rc = Abort;
        return rc;
    }

    RRLockEntry * entry = get_entry(access);
    uint64_t startt_get_latch = get_sys_clock();
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

    remove_tombstones();

    ts_t ts = txn->get_ts();
    if (type == R_REQ) {
        txn_man *retire_txn = version_header->retire;
        Version *read_version = version_header;
        if (owner == nullptr ){
            if (read_version->type != AT){
                access->tuple_version = read_version;
                goto final;
            } else {
                // read a version whose txn's timestamp < curr txn's timestamp
                if (ts == 0){
                    assign_ts(retire_txn->get_ts(), retire_txn);
                    ts = assign_ts(ts, txn);
                }
                while (true){
                    if (read_version->retire == nullptr && read_version->type == XP){
                        break;
                    } else {
                        if (read_version->retire != nullptr && read_version->retire->get_ts() < ts){
                            break;
                        }
                    }
                    read_version = read_version->next;
                }

                auto hreader = new HReader(txn);
                auto curr_hreader = read_version->read_queue;
                if (curr_hreader == nullptr){
                    hreader->prev = read_version;
                    read_version->read_queue = hreader;
                } else {
                    hreader->prev = read_version;
                    hreader->next = read_version->read_queue;
                    read_version->read_queue = hreader;
                }
                retire_txn = read_version->retire;
                if(retire_txn == txn){
                    rc = Abort;
                    goto final;
                }
                if (retire_txn == nullptr) {
                    access->tuple_version = read_version;
                    goto final;
                } else {
                    // if read a header, and the header is uncommitted
                    if (read_version == version_header){
                        auto mk = std::make_pair(txn, DepType::WRITE_READ_);
                        retire_txn->children.push_back(mk);
                        retire_txn->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                        auto mk_p = std::make_pair(retire_txn, DepType::WRITE_READ_);
                        txn->parents.insert(mk_p);
                    } else {
                        // if read a header.pre, and the pre is uncommitted
                        if (read_version->prev != nullptr){
                            retire_txn = read_version->prev->retire;
                            if (retire_txn != nullptr){
                                auto mk = std::make_pair(txn,  DepType::WRITE_READ_);
                                retire_txn->children.push_back(mk);
                                retire_txn->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                                auto mk_p = std::make_pair(retire_txn, DepType::WRITE_READ_);
                                txn->parents.insert(mk_p);
                            }
                        }
                    }
                }
            }
        } else {
            auto own_txn = owner->txn;
            if (own_txn != nullptr ){
                if (own_txn->status == ABORTED ) {
                    if (read_version->type != AT){
                        access->tuple_version = read_version;
                        goto final;
                    }
                } else if (own_txn->status == validating || own_txn->status == COMMITED) {
                    read_version = owner->access->tuple_version;
                    access->tuple_version = read_version;
                    goto final;
                } else if(own_txn->status == RUNNING){
                    auto own_ts = own_txn->get_ts();
                    own_ts = assign_ts(own_ts, own_txn);
                    ts = assign_ts(ts, txn);
                    if (a_higher_than_b(own_ts, ts)){
                        rc = WAIT;
                    }
                }
            }
        }

        if (!owner){
            if (!entry_list->empty()){
                auto read_en = find_write_in_waiter(ts);
                if (read_en != nullptr && read_en->txn!= nullptr){
                    rc = WAIT;
                }
            }
        }

        if (rc == WAIT){
            assert(ts > 0);
            entry->type = LOCK_SH;
            entry->status = LOCK_WAITER;
            add_to_waiters(ts, entry);
        }

        access->tuple_version = read_version;
    }else if (type == P_REQ) {
        txn_man *retire_txn = version_header->retire;
        Version *write_version = version_header;
        if (!owner ){
            if ((retire_txn == nullptr && write_version->type == XP) ||
                (retire_txn != nullptr && (retire_txn->status == COMMITED || retire_txn->status == validating))){
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
                rc = RCOK;
                goto final;
            }
        } else {
            auto own_txn = owner->txn;
            auto own_access = owner->access;
            if (own_access != nullptr && own_txn != nullptr){
                if (own_txn->status != ABORTED){
                    retire_txn = own_txn;
                    write_version = own_access->tuple_version;
                }
            }
        }

        if(retire_txn == txn){
            rc = Abort;
            goto final;
        }
        // assign timestamp
        if (ts == 0) {
            if (retire_txn != nullptr){
                auto retire_txn_ts = retire_txn->get_ts();
                assign_ts(retire_txn_ts, retire_txn);

                // if current header has latest readers, check readers and assign the ts
                auto readers = write_version->read_queue;
                HReader *dep_read_ = nullptr;
                if (readers != nullptr) {     // dependend on reader
                    dep_read_ = readers;
                    while (dep_read_ != nullptr) {
                        auto dep_read_txn_ = dep_read_->cur_reader;
                        if (dep_read_txn_ != nullptr) {
                            auto dep_ts = dep_read_txn_->get_ts();
                            if (dep_ts == 0) {
                                assign_ts(0, dep_read_txn_);
                            }
                        }
                        dep_read_ = dep_read_->next;
                    }
                }
            }
            ts = assign_ts(ts, txn);
        }

        // detect conflicts, if need to wound or need to rebirth
        wound_rebirth(ts, txn, type);
        if (txn->status == ABORTED){
            rc = Abort;
            goto final;
        }

        new_version->retire = txn;
        new_version->type = WR;
        access->tuple_version = new_version;

        assert(ts > 0);
        rc = WAIT;
        txn->lock_ready = false;
        entry->type = LOCK_EX;
        entry->has_write = false;
        entry->status = LOCK_WAITER;
        entry->access = access;
        entry->txn = txn;
        add_to_waiters(ts, entry);
    }

    if (txn->lock_abort || txn->status == ABORTED){
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

    //bring next waiter
    if (bring_next(txn, txn)) {
        rc = RCOK;
    }

    final:
#if PF_CS
    uint64_t timespan2 = get_sys_clock() - startt_get_latch;
    INC_STATS(txn->get_thd_id(), time_get_cs, timespan2);
    txn->wait_latch_time = txn->wait_latch_time + timespan2;
#endif

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

    // remove the aborted txn, GC
    remove_tombstones();

#if PASSIVE_RETIRE
    // passive retire the owner
    if (owner != nullptr){
    #if PF_CS
        uint64_t timestart_passive = get_sys_clock();
    #endif
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
                        version_header = access_version;
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

    // if any waiter can join the owners, just do it!
    for (auto it = entry_list->begin(); it != entry_list->end(); ++it) {
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

                // add owner depended on the retired tail
                auto retire_tail = version_header->retire;
                auto readers = version_header->read_queue;
                bool has_depend = false;
                if (retire_tail != nullptr) {
                    if (readers != nullptr) {
                        HReader *dep_read_ = readers;
                        while (dep_read_ != nullptr) {
                            auto dep_read_txn_ = dep_read_->cur_reader;
                            if (dep_read_txn_ != nullptr && dep_read_txn_->status == RUNNING) {
                                if (dep_read_txn_->get_thd_id() != owner->txn->get_thd_id()) {
                                    auto mk = std::make_pair(owner->txn, DepType::READ_WRITE_);
                                    dep_read_txn_->children.push_back(mk);
                                    dep_read_txn_->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                                    auto retire_ts = dep_read_txn_->get_ts();
                                    if (dep_read_txn_->status == validating && owner->txn->get_ts() < retire_ts) {
                                        reassign_ts(owner->txn);
                                    }
                                    auto mk_p = std::make_pair(dep_read_txn_, DepType::READ_WRITE_);
                                    if (owner->txn != nullptr) {
                                        owner->txn->parents.insert(mk_p);
                                        has_depend = true;
                                    }
                                }
                            }

                            dep_read_ = dep_read_->next;
                        }
                    }
                    if (!has_depend) {
                        auto mk = std::make_pair(owner->txn, DepType::WRITE_WRITE_);
                        retire_tail->children.push_back(mk);
                        retire_tail->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                        auto retire_ts = retire_tail->get_ts();
                        if (retire_tail->status == validating && owner->txn->get_ts() < retire_ts) {
                            reassign_ts(owner->txn);
                        }
                        auto mk_p = std::make_pair(retire_tail, DepType::WRITE_WRITE_);
                        if (owner->txn != nullptr && owner->access != nullptr) {
                            auto owner_parents = owner->txn->parents;
                            if (retire_tail != nullptr && retire_tail->status != ABORTED) {
                                owner_parents.insert(mk_p);
                            }
                        }
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
                    has_txn = bring_out_waiter(entry, txn);
                    Version *read_version;
                    if (owner){
                        read_version = owner->access->tuple_version;
                    } else {
                        read_version = version_header;
                    }
                    auto retire_txn = read_version->retire;
                    auto en_txn = entry->txn;
                    if (retire_txn != nullptr){
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
                        auto mk = std::make_pair(en_txn,  DepType::WRITE_READ_);
                        retire_txn->children.push_back(mk);
                        retire_txn->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                        auto mk_p = std::make_pair(retire_txn, DepType::WRITE_READ_);
                        en_txn->parents.insert(mk_p);
                    }

                    entry->access->tuple_version = read_version;
                }
            }
        } else {
            break;
        }
    }


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
        bring_next(nullptr, nullptr);
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



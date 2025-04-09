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

#if LATCH == LH_SPINLOCK
    spinlock_row = new pthread_spinlock_t;
    pthread_spin_init(spinlock_row, PTHREAD_PROCESS_SHARED);
#else
    latch_row = new mcslock();
#endif

    latest = version_header;
    owner = nullptr;
    entry_list = new std::list<RRLockEntry *>();
}


RC Row_rr::access(txn_man * txn, TsType type, Access * access){
    // Optimization for read_only long transaction.
    uint64_t startt_r_w = get_sys_clock();
    if(txn->is_long || txn->read_only){
        ts_t ts = txn->get_ts();
        ts = assign_ts(ts, txn);
        if (ts >= latest->begin_ts){
            if (latest->end_ts == INF || ts < latest->end_ts){
                if(latest->type == XP){
                    access->tuple_version = latest;
                    txn->lock_ready = true;
//                    auto max_ts = std::max(latest->begin_ts, ts);
//                    txn->set_ts(max_ts);
#if PF_CS
                    INC_STATS(txn->get_thd_id(), time_read_write,  (get_sys_clock() - startt_r_w));
                    txn->wait_latch_time = txn->wait_latch_time + (get_sys_clock() - startt_r_w);
#endif
                    return RCOK;
                }
            }
        }else{
            // prefetching
            Version* read_only_version = latest;
            while (true){
                if (read_only_version == nullptr){
                    txn->lock_abort = true;
                    return Abort;
                }
                if(read_only_version->type == XP){
#if PREFETCH
                    assert(read_only_version->retire == NULL);
                    access->tuple_version = read_only_version;
                    break;
#else
                    if (ts >= read_only_version->begin_ts){
                        assert(read_only_version->retire == NULL);
                        access->tuple_version = read_only_version;
                        break;
                    }
#endif
                }

                read_only_version = read_only_version->next;
            }

            assert(access->tuple_version != nullptr);
            assert(access->tuple_version->data != nullptr);
//            auto max_ts = std::max(access->tuple_version->begin_ts, ts);
//            txn->set_ts(max_ts);
            txn->lock_ready = true;
#if PF_CS
            INC_STATS(txn->get_thd_id(), time_read_write,  (get_sys_clock() - startt_r_w));
            txn->wait_latch_time = txn->wait_latch_time + (get_sys_clock() - startt_r_w);
#endif
            return RCOK;
        }
    }

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
#if PF_CS
    uint64_t startt_get_latch = get_sys_clock();
#endif
    RRLockEntry * entry = get_entry(access);
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

    auto ret = remove_tombstones();
    if (!ret){
        rc = Abort;
#if PF_CS
        uint64_t timespan2 = get_sys_clock() - startt_get_latch;
        INC_STATS(txn->get_thd_id(), time_get_cs, timespan2);
        txn->wait_latch_time = txn->wait_latch_time + timespan2;
#endif
        unlock_row(txn);
        return rc;
    }

    ts_t ts = txn->get_ts();
    if (type == R_REQ) {
        txn_man *retire_txn = version_header->retire;
        Version *read_version = version_header;
            if (owner){
                auto own_txn = owner->txn;
                auto own_ts = own_txn->get_ts();
                own_ts = assign_ts(own_ts, own_txn);
                ts = assign_ts(ts, txn);
                if (a_higher_than_b(own_ts, ts)){
                    rc = WAIT;
                } else {
                    read_version = owner->access->tuple_version;
                }
            }

            if (rc != WAIT){
                if (!owner){
                    while (true){
                        if (read_version == nullptr){
                            rc = Abort;
                            goto final;
                        }
                        if (read_version == latest){
                            break;
                        }
                        if (read_version->type == XP){
                            ts = assign_ts(ts, txn);
                            if (read_version->begin_ts < ts){
                                break;
                            }
                        }else if (read_version->type == WR){
                            retire_txn = read_version->retire;
                            assign_ts(retire_txn->get_ts(), retire_txn);
                            ts = assign_ts(ts, txn);
                            if (retire_txn != nullptr && retire_txn->get_ts() < ts ){
                                break;
                            }
                        } else{
                        }

                        read_version = read_version->next;
                    }
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
                auto thd_id = txn->get_thd_id();
                if(retire_txn == txn){
                    rc = Abort;
                    goto final;
                }
                if (retire_txn == nullptr) {
                    access->tuple_version = read_version;
                    goto final;
                } else {
                    // if read a header, and the header is uncommitted
                    if (read_version == version_header || (owner != nullptr && read_version == owner->access->tuple_version)){
#if CHILDOPT
                        retire_txn->children.fetch_or(1ULL << thd_id, std::memory_order_relaxed);
#else
                        auto mk = std::make_pair(txn, DepType::WRITE_READ_);
                        retire_txn->children.push_back(mk);
#endif
                        retire_txn->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                        auto mk_p = std::make_pair(retire_txn, DepType::WRITE_READ_);
                        txn->parents.push_back(mk_p);
                    } else {
                        // if read a header.pre, and the pre is uncommitted
                        if (read_version->prev != nullptr){
                            retire_txn = read_version->prev->retire;
                            if (retire_txn != nullptr){
#if CHILDOPT
                                retire_txn->children.fetch_or(1ULL << thd_id, std::memory_order_relaxed);
#else
                                auto mk = std::make_pair(txn,  DepType::WRITE_READ_);
                                retire_txn->children.push_back(mk);
#endif
                                retire_txn->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                                auto mk_p = std::make_pair(retire_txn, DepType::WRITE_READ_);
                                txn->parents.push_back(mk_p);
                            }
                        }
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
        if (!owner ) {
            if ((retire_txn == nullptr && write_version->type == XP) ||
                (retire_txn != nullptr && (retire_txn->status == COMMITED || retire_txn->status == validating))) {
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
            if (own_access != nullptr && own_txn != nullptr) {
                if (own_txn->status != ABORTED) {
                    retire_txn = own_txn;
                    write_version = own_access->tuple_version;
                }
            }
        }

        if(retire_txn == txn){
            if (owner!= nullptr && txn == owner->txn){
                new_version->retire = txn;
                new_version->type = WR;
                access->tuple_version = new_version;
                goto final;
            }else{
                rc = Abort;
                goto final;
            }
        }

        // assign timestamp
        ts = txn->get_ts();
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
    auto ret = remove_tombstones();
    if (!ret){
        return false;
    }

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

    uint64_t retire_cs_ = get_sys_clock();
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
#if CHILDOPT
                                    auto thd_id = owner->txn->get_thd_id();
                                    dep_read_txn_->children.fetch_or(1ULL << thd_id, std::memory_order_relaxed);
#else
                                    auto mk = std::make_pair(owner->txn, DepType::READ_WRITE_);
                                    dep_read_txn_->children.push_back(mk);
#endif
                                    dep_read_txn_->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                                    auto retire_ts = dep_read_txn_->get_ts();
                                    if (dep_read_txn_->status == validating && owner->txn->get_ts() < retire_ts) {
                                        reassign_ts(owner->txn);
                                    }
                                    auto mk_p = std::make_pair(dep_read_txn_, DepType::READ_WRITE_);
                                    if (owner->txn != nullptr) {
                                        owner->txn->parents.push_back(mk_p);
                                        has_depend = true;
                                    }
                                }
                            }

                            dep_read_ = dep_read_->next;
                        }
                    }
                    if (!has_depend) {
#if CHILDOPT
                        auto thd_id = owner->txn->get_thd_id();
                        retire_tail->children.fetch_or(1ULL << thd_id, std::memory_order_relaxed);
#else
                        auto mk = std::make_pair(owner->txn, DepType::WRITE_WRITE_);
                        retire_tail->children.push_back(mk);
#endif
                        retire_tail->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                        auto retire_ts = retire_tail->get_ts();
                        if (retire_tail->status == validating && owner->txn->get_ts() < retire_ts) {
                            reassign_ts(owner->txn);
                        }
                        auto mk_p = std::make_pair(retire_tail, DepType::WRITE_WRITE_);
                        if (owner->txn != nullptr && owner->access != nullptr) {
                            auto& owner_parents = owner->txn->parents;
                            if (retire_tail != nullptr && retire_tail->status != ABORTED) {
                                owner_parents.push_back(mk_p);
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
#if CHILDOPT
                        auto thd_id = en_txn->get_thd_id();
                        retire_txn->children.fetch_or(1ULL << thd_id, std::memory_order_relaxed);
#else
                        auto mk = std::make_pair(en_txn,  DepType::WRITE_READ_);
                        retire_txn->children.push_back(mk);
#endif
                        retire_txn->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                        auto mk_p = std::make_pair(retire_txn, DepType::WRITE_READ_);
                        en_txn->parents.push_back(mk_p);
                    }

                    en_txn->lock_ready = true;

                    entry->access->tuple_version = read_version;
                    assert(read_version != nullptr);
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



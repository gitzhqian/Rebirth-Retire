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
    waiters_head = NULL;
    waiters_tail = NULL;
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

    bool wait = false;
    txn->lock_ready = true;
    LockEntry * entry = get_entry(access);
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
        if (owner == nullptr || owner->txn == nullptr || owner->status == LOCK_DROPPED || owner->status == LOCK_RETIRED){
            if ((retire_txn == nullptr && read_version->type == XP) ||
                (retire_txn != nullptr && (retire_txn->status == COMMITED || retire_txn->status == validating))){
                access->tuple_version = read_version;
                goto final;
            }
        }

        bool has_read = false;
        if(owner != nullptr && (owner->status == LOCK_OWNER || owner->status == LOCK_RETIRED) &&
           owner->txn != nullptr && owner->txn->status != ABORTED) {
            auto own_ts = owner->txn->get_ts();
            own_ts = assign_ts(own_ts, owner->txn);
            ts = assign_ts(ts, txn);
            if (a_higher_than_b(own_ts, ts)){
                read_version = owner->access->tuple_version;
                has_read = true;
                HReader *hreader = new HReader(txn);
                auto curr_hreader = read_version->read_queue;
                if (curr_hreader == nullptr){
                    hreader->prev = read_version;
                    read_version->read_queue = hreader;
                } else {
                    hreader->prev = read_version;
                    hreader->next = read_version->read_queue;
                    read_version->read_queue = hreader;
                }
            }
        } else if (waiters_head != nullptr && waiters_head->txn != nullptr && waiters_head->txn->status != ABORTED){
            auto read_en = find_write_in_waiter(ts);
            if (read_en != nullptr && read_en->txn!= nullptr){
                read_version = read_en->access->tuple_version;
                has_read = true;
            }
        }

        if (has_read){
            assert(ts > 0);
            txn->lock_ready = false;
            txn->lock_abort = false;
            wait = true;
            entry->type = LOCK_SH;
            entry->txn = txn;
            entry->access = access;
            add_to_waiters(ts, entry);
        } else {
            retire_txn = version_header->retire;
            read_version = version_header;
            if (retire_txn == nullptr || retire_txn->status == COMMITED || retire_txn->status == validating){
                access->tuple_version = read_version;
                goto final;
            }

            if (ts == 0){
                assign_ts(retire_txn->get_ts(), retire_txn);
                ts = assign_ts(ts, txn);
            }
            // read a version whose txn's timestamp < curr txn's timestamp
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

            HReader *hreader = new HReader(txn);
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
            assert(retire_txn != txn);
            if ( retire_txn != nullptr) {
                // if read a header, and the header is uncommitted
                if (read_version == version_header){
                    if (retire_txn->status == validating && txn->get_ts() < retire_txn->get_ts()){
                        reassign_ts(txn);
                        COMPILER_BARRIER
                    }
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
                            if (retire_txn->status == validating && txn->get_ts() < retire_txn->get_ts()){
                                reassign_ts(txn);
                                COMPILER_BARRIER
                            }
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


        access->tuple_version = read_version;
    }else if (type == P_REQ) {
        txn_man *retire_txn = version_header->retire;
        Version *write_version = version_header;
        if (owner == nullptr || owner->txn == nullptr || owner->status == LOCK_DROPPED || owner->status == LOCK_RETIRED ){
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
                goto final;
            }
        }

        if(owner != nullptr && (owner->status == LOCK_OWNER || owner->status == LOCK_RETIRED) &&
           owner->txn != nullptr && owner->txn->status != ABORTED) {
            retire_txn = owner->txn;
            write_version = owner->access->tuple_version;
        }

        assert(retire_txn != txn);
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
#if PF_CS
            uint64_t timespan2 = get_sys_clock() - startt_get_latch;
            INC_STATS(txn->get_thd_id(), time_get_cs, timespan2);
            txn->wait_latch_time = txn->wait_latch_time + timespan2;
#endif
            unlock_row(txn);
            return rc;
        }

        new_version->retire = txn;
        new_version->type = WR;
        access->tuple_version = new_version;

        assert(ts > 0);
        txn->lock_ready = false;
        txn->lock_abort = false;
        wait = true;
        entry->type = LOCK_EX;
        entry->txn = txn;
        entry->access = access;
        entry->has_write = false;
        add_to_waiters(ts, entry);
    }

    if (txn->status == ABORTED){
        rc = Abort;
#if PF_CS
        uint64_t timespan2 = get_sys_clock() - startt_get_latch;
        INC_STATS(txn->get_thd_id(), time_get_cs, timespan2);
        txn->wait_latch_time = txn->wait_latch_time + timespan2;
#endif
        unlock_row(txn);
        return rc;
    }

    if (!wait){
        assert(rc == RCOK);
    } else {
        rc = WAIT;
    }

    //bring next waiter
    if (bring_next(txn)){
        rc = RCOK;
        txn->lock_ready = true;
    }

    final:
#if PF_CS
    uint64_t timespan2 = get_sys_clock() - startt_get_latch;
    INC_STATS(txn->get_thd_id(), time_get_cs, timespan2);
    txn->wait_latch_time = txn->wait_latch_time + timespan2;
#endif

    unlock_row(txn);
    COMPILER_BARRIER

    return  rc;
}

bool Row_rr::bring_next(txn_man *txn) {
    bool has_txn = false;
    LockEntry *entry = waiters_head;
    LockEntry *next = NULL;

    // remove the aborted txn, GC
    remove_tombstones();

#if PASSIVE_RETIRE
    // passive retire the owner
    if (owner != nullptr){
#if PF_CS
    uint64_t timestart_passive = get_sys_clock();
#endif
        while (!owner->has_write){
            if (owner->status == LOCK_DROPPED){
                break;
            }
            if (owner->txn != nullptr && owner->txn->status == ABORTED){
                break;
            }

            PAUSE
        }

        if (owner->status == LOCK_OWNER && owner->txn != nullptr){
            // move it out of the owner
            owner->status = LOCK_RETIRED;
            // passive retire the owner, move the owner to the retire tail
            version_header->prev = owner->access->tuple_version;
            version_header = owner->access->tuple_version;
        }

#if PF_CS
    // the timespan of passive-retire wait, add it to the time_wait
    uint64_t timeend_passive = get_sys_clock();
    uint64_t timespan = timeend_passive - timestart_passive;
    INC_TMP_STATS(owner->txn->get_thd_id(), time_wait, timespan);
    owner->txn->wait_passive_retire = owner->txn->wait_passive_retire + timespan;
#endif

        owner = nullptr;
    }
#endif

    // if any waiter can join the owners, just do it!
    while (entry) {
        next = entry->next;
        if (!owner){
            if (entry->type == LOCK_EX){
                // promote a waiter to become the owner
                if (entry->txn == nullptr || entry->txn == version_header->retire ||
                    (entry->txn != nullptr && entry->txn->status == ABORTED)){
                    bring_out_waiter(entry, nullptr);
                    break;
                }

                assert(entry->txn != version_header->retire);
                owner = entry;
                owner->status = LOCK_OWNER;

                // add owner depended on the retired tail
                auto retire_tail = version_header->retire;
                auto readers = version_header->read_queue;
                bool has_depend = false;
                if (retire_tail != nullptr && retire_tail->status == RUNNING) {
                    if (readers != nullptr) {
                        HReader *dep_read_ = readers;
                        while (dep_read_ != nullptr) {
                            auto dep_read_txn_ = dep_read_->cur_reader;
                            if (dep_read_txn_ != nullptr && dep_read_txn_->status == RUNNING) {
                                if(dep_read_txn_->get_thd_id() != owner->txn->get_thd_id()){
                                    auto mk = std::make_pair(owner->txn,  DepType::READ_WRITE_);
                                    dep_read_txn_->children.push_back(mk);
                                    dep_read_txn_->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                                    auto retire_ts = dep_read_txn_->get_ts();
                                    if(dep_read_txn_->status == validating && owner->txn->get_ts() < retire_ts){
                                        reassign_ts(owner->txn);
                                        COMPILER_BARRIER
                                    }
                                    auto mk_p = std::make_pair(dep_read_txn_, DepType::READ_WRITE_);
                                    owner->txn->parents.insert(mk_p);
                                    has_depend = true;
                                }
                            }

                            dep_read_ = dep_read_->next;
                        }
                    }
                    if (!has_depend){
                        assert(retire_tail->get_thd_id() != owner->txn->get_thd_id());
                        auto mk = std::make_pair(owner->txn,  DepType::WRITE_WRITE_);
                        retire_tail->children.push_back(mk);
                        retire_tail->timestamp_v.fetch_add(1, std::memory_order_relaxed);
                        auto retire_ts = retire_tail->get_ts();
                        if(retire_tail->status == validating && owner->txn->get_ts() < retire_ts){
                            reassign_ts(owner->txn);
                            COMPILER_BARRIER
                        }
                        auto mk_p = std::make_pair(retire_tail, DepType::WRITE_WRITE_);
                        owner->txn->parents.insert(mk_p);
                    }
                }

                if ((retire_tail!= nullptr && retire_tail->status == ABORTED)){
                    if (!has_depend){
                        owner->txn->parents.unsafe_erase(retire_tail);
                    }
                }

                // there may exists txn whose type is AT
                owner->access->old_version = version_header;
                owner->access->tuple_version->next = version_header;
                owner->has_write = true;

                has_txn = bring_out_waiter(entry, nullptr);

                if (entry->txn == nullptr || entry->txn->lock_abort){
                    owner = nullptr;
                }

                break;
            } else {
                // may promote multiple readers
                entry->status = LOCK_RETIRED;

                has_txn = bring_out_waiter(entry, nullptr);
            }

            entry = next;
        }else{
            break;
        }
    }

    return has_txn;
}

RC Row_rr::active_retire(LockEntry * entry){
    RC rc = RCOK;

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

    // remove the aborted txn, GC
    remove_tombstones();

    if (entry->status == LOCK_OWNER && entry->txn != nullptr && entry->txn->status != ABORTED){
        assert(owner == entry);
        // move it out of the owner
        entry->status = LOCK_RETIRED;
        // active retire the owner, move the owner to the retire tail
        version_header->prev = entry->access->tuple_version;
        version_header = entry->access->tuple_version;

        owner = nullptr;
    }else {
        if (entry->txn != nullptr && entry->txn->status == ABORTED){
            if(owner == entry){
                owner = nullptr;
            }
            rc = Abort;
        }
        assert(entry->status == LOCK_DROPPED || entry->status == LOCK_OWNER || entry->txn->lock_abort
               || entry->txn->status == ABORTED);
    }

#if PF_CS
    uint64_t timespan1 = get_sys_clock() - startt_retire;
    INC_STATS(entry->txn->get_thd_id(), time_retire_cs, timespan1);
    entry->txn->wait_latch_time = entry->txn->wait_latch_time + timespan1;
#endif

    bring_next(nullptr);

    unlock_row(entry->txn);
    COMPILER_BARRIER

    return rc;
}

#endif



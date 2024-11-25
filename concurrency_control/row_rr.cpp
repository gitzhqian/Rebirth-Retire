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

    version_header->dynamic_txn_ts = (volatile ts_t *)_mm_malloc(sizeof(ts_t), 64);
    version_header->dynamic_txn_ts = new ts_t(0);
    version_header->type = XP;
    version_header->read_queue = NULL;

    // pointer must be initialized
    version_header->prev = NULL;
    version_header->next = NULL;
    version_header->retire = NULL;
//    version_header->retire_ID = 0;

#ifdef ABORT_OPTIMIZATION
    version_header->version_number = 0;
#endif

#if VERSION_CHAIN_CONTROL
    // Restrict the length of version chain.
    threshold = 0;
#endif

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
            new_version->version_number = version_header->version_number;
        } else{
            new_version = reserve_version;
            new_version->init();
            new_version->next = nullptr;
            new_version->version_number = version_header->version_number;
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
    ts_t ts = txn->get_ts();
    txn->lock_ready = true;
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

    if (type == R_REQ) {
        txn_man *retire_txn = version_header->retire;
        Version *read_version = version_header;
        while (version_header) {
            if (version_header->type == AT){
                version_header = version_header->next;
#if ABORT_OPTIMIZATION
                Version* version_retrieve = version_header;
                while(version_retrieve->begin_ts == UINT64_MAX){
                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                    version_retrieve = version_retrieve->next;
                }
                version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
#endif
                continue;
            }

            retire_txn = version_header->retire;
            read_version = version_header;
            if (retire_txn == nullptr) { // committed version
                break;
            } else { // uncommitted version
                assert(retire_txn != txn);
                status_t temp_status = retire_txn->status;
                if ( temp_status == COMMITED || temp_status == validating ) {
                    break;
                } else if (temp_status == RUNNING ) {       // record dependency, read an uncommited version,
                    // assign timestamp
                    if (ts == 0){
                        auto retire_txn_ts = retire_txn->get_ts();
                        if (retire_txn_ts == 0){
                            assign_ts(0, retire_txn);
                        }
                        ts = assign_ts(ts, txn);
                    }
                    // read a version whose txn's timestamp < curr txn's timestamp
                    while (true){
                        if (read_version->type != AT){
                            if (read_version->retire == nullptr){
                                break;
                            } else {
                                if (read_version->retire->get_ts() <= ts){
//                                    access->tuple_version = read_version;
                                    break;
                                }
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
                    if ( retire_txn != nullptr) {
                        if (read_version == version_header){
                            retire_txn->PushDependency(txn, txn->get_txn_id(), DepType::WRITE_READ_);  //dependent on me(retire)
                            txn->insert_i_dependency_on(retire_txn, DepType::WRITE_READ_);
                            txn->rr_semaphore ++;
                        } else {
                            if (read_version->prev != nullptr){
                                retire_txn = read_version->prev->retire;
                                if (retire_txn != nullptr){
                                    retire_txn->PushDependency(txn, txn->get_txn_id(), DepType::WRITE_READ_);  //dependent on me(retire)
                                    txn->insert_i_dependency_on(retire_txn, DepType::WRITE_READ_);
                                    txn->rr_semaphore ++;
                                }
                            }
                        }
                    }

                    assert(ts > 0);
                    txn->lock_ready = false;
                    wait = true;
                    LockEntry * entry = get_entry(access);
                    entry->type = LOCK_SH;
                    entry->status = LOCK_WAITER;
                    entry->txn = txn;
                    add_to_waiters(ts, entry);

                    break;
                } else if (temp_status == ABORTED) {
                    version_header = version_header->next;
#if ABORT_OPTIMIZATION
                    Version* version_retrieve = version_header;
                    while(version_retrieve->begin_ts == UINT64_MAX){
                        version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                        version_retrieve = version_retrieve->next;
                    }
                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
#endif
                    continue;
                }

            }
        }

        access->tuple_version = read_version;
    }else if (type == P_REQ) {
        txn_man *retire_txn = version_header->retire;
        while (version_header) {
            if (version_header->type == AT){
                version_header = version_header->next;
#if ABORT_OPTIMIZATION
                Version* version_retrieve = version_header;
                while(version_retrieve->begin_ts == UINT64_MAX){
                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                    version_retrieve = version_retrieve->next;
                }
                version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
#endif
                continue;
            }

            retire_txn = version_header->retire;
            if (retire_txn == nullptr) {  //committed
                break;
            } else {    // committed
                assert(retire_txn != txn);
                status_t temp_status = retire_txn->status;
                if (temp_status == COMMITED || temp_status == validating ) {
                    break;
                } else if (temp_status == RUNNING) {       // record dependency
                    // assign timestamp
                    if (ts == 0){
                        auto retire_txn_ts = retire_txn->get_ts();
                        if (retire_txn_ts == 0){
                            assign_ts(0, retire_txn);
                        }
                        //if current header has latest readers, check readers and assign the ts
                        auto readers = version_header->read_queue;
                        HReader *dep_read_ = nullptr;
                        if (readers != nullptr) {   //dependend on reader
                            dep_read_ = readers;
                            while (dep_read_ != nullptr) {
                                auto dep_read_txn_ = dep_read_->cur_reader;
                                if (dep_read_txn_ != nullptr) {
                                    auto dep_ts = dep_read_txn_->get_ts();
                                    if (dep_ts == 0){
                                        assign_ts(0, dep_read_txn_);
                                    }
                                }
                                dep_read_ = dep_read_->next;
                            }
                        }

                        ts = assign_ts(ts, txn);
                    }

                    //check ts, if need to wound or need to rebirth
                    auto ret = wound_rebirth(ts, txn, type);
                    if (ret){
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
                    }

                    assert(ts > 0);
                    txn->lock_ready = false;
                    wait = true;
                    LockEntry * entry = get_entry(access);
                    entry->type = LOCK_EX;
                    entry->status = LOCK_WAITER;
                    entry->txn = txn;
                    add_to_waiters(ts, entry);

                    break;
                }  else if (temp_status == ABORTED) {
                    version_header = version_header->next;
#if ABORT_OPTIMIZATION
                    Version* version_retrieve = version_header;
                    while(version_retrieve->begin_ts == UINT64_MAX){
                        version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                        version_retrieve = version_retrieve->next;
                    }
                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
#endif
                    continue;
                }
            }
        }
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

    //bring next
//    if (owner == nullptr || owner->status == LOCK_RETIRED){
    bring_next();
//    }

#if PF_CS
    uint64_t timespan2 = get_sys_clock() - startt_get_latch;
    INC_STATS(txn->get_thd_id(), time_get_cs, timespan2);
    txn->wait_latch_time = txn->wait_latch_time + timespan2;
#endif
    unlock_row(txn);
    COMPILER_BARRIER

    if (txn->status == ABORTED){
        rc = Abort;
        return rc;
    }
    if (!wait){
        if (type == P_REQ) {
            access->old_version = version_header;
            new_version->next = version_header;
            new_version->retire = txn;
            new_version->dynamic_txn_ts = (volatile ts_t *)&txn->timestamp;
            new_version->type = WR;
#if ABORT_OPTIMIZATION
            new_version->version_number = version_header->version_number + CHAIN_NUMBER_ADD_ONE;
#endif
            version_header->prev = new_version;
            version_header = new_version;
            assert(version_header->end_ts == INF);
            access->tuple_version = new_version;
        }

        return rc;
    }

    //no retire
    bool retire = true;
    if (!retire){
        if (wait && !retire){
            while (version_header->retire != nullptr){
                PAUSE
            }
            if (txn->status == ABORTED){
                rc = Abort;
                return rc;
            }
            if (type == P_REQ) {
                access->old_version = version_header;
                new_version->next = version_header;
                new_version->retire = txn;
                new_version->dynamic_txn_ts = (volatile ts_t *)&txn->timestamp;
                new_version->type = WR;
#if ABORT_OPTIMIZATION
                new_version->version_number = version_header->version_number + CHAIN_NUMBER_ADD_ONE;
#endif
                version_header->prev = new_version;
                version_header = new_version;
                assert(version_header->end_ts == INF);
                access->tuple_version = new_version;
            }
        }

        return rc;
    }


    //otherwise retire, wait for lock
    uint64_t start_wait = get_sys_clock();
    while (true){
        if (txn->lock_ready){
            break;
        }
        if (type == P_REQ) {
            if (owner == nullptr  ){
                break;
            } else{
                if (owner->status == LOCK_RETIRED ||  owner->status == LOCK_DROPPED){
                    break;;
                }
            }
        }else{
            if (access->tuple_version->retire == nullptr){
                break;
            } else {
                if (access->tuple_version->retire->lock_ready){
                    break;
                }
            }
        }
        if (txn->status == ABORTED){
            rc = Abort;
            break;
        }
        if (get_sys_clock() - start_wait > g_timeout) {
            rc = Abort;
            break;
        }
    }

#if PF_CS
    uint64_t timespan3 = get_sys_clock() - start_wait;
    INC_TMP_STATS(txn->get_thd_id(), time_wait, timespan3);  // when abort, dec from the abort time
//    txn->wait_latch_time = txn->wait_latch_time + timespan3;
#endif

    if (rc == Abort){
        return  rc;
    }

    if (type == R_REQ) {
        access->lock_entry->status = LOCK_RETIRED;
        return rc;
    }

    uint64_t startt_retire = get_sys_clock();
    lock_row(txn);
    COMPILER_BARRIER
#if PF_CS
    uint64_t end_retire = get_sys_clock();
    uint64_t  timespan4 = end_retire - startt_retire;
    INC_STATS(txn->get_thd_id(), time_retire_latch,  timespan4);
    txn->wait_latch_time = txn->wait_latch_time + timespan4;
    startt_retire = end_retire;
#endif
    if (txn->status == ABORTED){
        rc = Abort;
#if PF_CS
        uint64_t timespan5 = get_sys_clock() - startt_retire;
        INC_STATS(txn->get_thd_id(), time_retire_cs, timespan5);
        txn->wait_latch_time = txn->wait_latch_time + timespan5;
#endif
        unlock_row(txn);
        return rc;
    }
    access->lock_entry->status = LOCK_RETIRED;
    txn->lock_ready = true;

    auto retire_txn = version_header->retire;
    while (version_header) {
        retire_txn = version_header->retire;
        if (!retire_txn) {  //committed
            break;
        }  else { // uncommitted
            assert(retire_txn != txn);
            status_t temp_status = retire_txn->status;
            if (temp_status == COMMITED || temp_status == validating ) {
                break;
            } else if (temp_status == RUNNING) {       // record dependency
                auto readers = version_header->read_queue;
                if (readers != nullptr) {   //dependend on reader
                    HReader *dep_read_ = readers;
                    while (dep_read_ != nullptr) {
                        auto dep_read_txn_ = dep_read_->cur_reader;
                        if (dep_read_txn_ != nullptr) {
                            dep_read_txn_->PushDependency(txn, txn->get_txn_id(), DepType::READ_WRITE_);
                            txn->insert_i_dependency_on(dep_read_txn_, DepType::READ_WRITE_);
                            txn->rr_semaphore ++;
                        }
                        dep_read_ = dep_read_->next;
                    }
                }else {
                    retire_txn->PushDependency(txn, txn->get_txn_id(), DepType::WRITE_WRITE_);
                    txn->insert_i_dependency_on(retire_txn, DepType::WRITE_WRITE_);
                    txn->rr_semaphore ++;
                }

                break;
            }  else if (temp_status == ABORTED) {
                version_header = version_header->next;
#if ABORT_OPTIMIZATION
                Version* version_retrieve = version_header;
                while(version_retrieve->begin_ts == UINT64_MAX){
                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                    version_retrieve = version_retrieve->next;
                }
                version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
#endif
//                        assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                continue;
            }
        }
    }

    //to retire
    access->old_version = version_header;
    new_version->next = version_header;
    new_version->retire = txn;
    new_version->dynamic_txn_ts = (volatile ts_t *)&txn->timestamp;
    new_version->type = WR;
#if ABORT_OPTIMIZATION
    new_version->version_number = version_header->version_number + CHAIN_NUMBER_ADD_ONE;
#endif
    version_header->prev = new_version;
    version_header = new_version;
    assert(version_header->end_ts == INF);
    access->tuple_version = new_version;


#if PF_CS
    uint64_t timespan5 = get_sys_clock() - startt_retire;
    INC_STATS(txn->get_thd_id(), time_retire_cs, timespan5);
    txn->wait_latch_time = txn->wait_latch_time + timespan5;
#endif

    //bring next
//    if (owner == nullptr || owner->status == LOCK_RETIRED){
    bring_next();
//    }

    unlock_row(txn);
    COMPILER_BARRIER

    access->lock_entry->status = LOCK_DROPPED;


    return rc;
}


#endif



//
// Created by root on 2024/7/22.
//
#include "txn.h"
#include "row.h"
#include "row_rr.h"
#include "manager.h"
#include <mm_malloc.h>
#include <unordered_set>
#include "thread.h"


#if CC_ALG == REBIRTH_RETIRE

RC txn_man::validate_rr(RC rc) {

    /** * Update status. */
    if(rc == Abort || status == ABORTED){
//        printf("rc:%d. \n", rc);
        abort_process(this);

        return Abort;
    }

    this->rr_serial_id =  this->get_ts() ;
    if(this->rr_serial_id == 0){
        // traverse the reads and writes, max of read version and max+1 of write version
        for(int rid = 0; rid < row_cnt; rid++) {
            auto new_version = accesses[rid]->tuple_version;
            auto old_version = accesses[rid]->old_version;
            if (accesses[rid]->type == RD) {
                if (new_version->type == AT) continue;
                this->rr_serial_id = std::max(new_version->begin_ts, this->rr_serial_id);
            }
            if (accesses[rid]->type == WR){
                if (old_version->type == AT) continue;
                auto ts = (*old_version->dynamic_txn_ts) + 10;
                this->rr_serial_id = std::max(ts, this->rr_serial_id);
            }
        }

        this->rr_serial_id = this->rr_serial_id +1;
    }

    assert(this->rr_serial_id != INF);


    std::unordered_map<uint64_t, DepType> i_depents;
    auto direct_depents = this->rr_dependency;
 
    uint64_t i_dependency_on_size = i_dependency_on.size();
    assert(this->rr_semaphore == i_dependency_on_size);


    uint64_t starttime = get_sys_clock();
    while(true) {
        if(status == ABORTED){

            rc = Abort;
            break;
        }

        if (this->rr_semaphore  == 0) {
            break;
        }
        else {
            for (auto it = i_dependency_on.begin(); it != i_dependency_on.end(); ++it) {
                auto depend_txn = (*it).first;
                if (depend_txn == nullptr || depend_txn->status == COMMITED) {
                    if (this->rr_semaphore > 0){
                        this->rr_semaphore -- ;
                    }
                }

                if (depend_txn->status == ABORTED){
                    if ((*it).second != READ_WRITE_){

                        this->status = ABORTED;
                        break;
                    } else{
                        if (this->rr_semaphore > 0){
                            this->rr_semaphore -- ;
                        }
                    }
                }

                if (depend_txn->status == validating ){
                    auto depend_txn_ts = depend_txn->get_ts();
                    if (depend_txn_ts < this->rr_serial_id){
                        if (this->rr_semaphore > 0){
                            this->rr_semaphore -- ;
                        }
                    } else{
                        auto itr = i_depents.find(depend_txn->get_txn_id());
                        if ( itr != i_depents.end()){

                            this->status = ABORTED;
                            break;
                        } else {
                            this->set_ts(depend_txn_ts+1);
                            this->rr_serial_id = depend_txn_ts+1;
                            if (this->rr_semaphore > 0){
                                this->rr_semaphore -- ;
                            }
                        }
                    }
                }
            }
        }
    }


#if PF_CS
    uint64_t endtime_commit = get_sys_clock();
    uint64_t timespan1 = endtime_commit - starttime;
    INC_STATS(this->get_thd_id(), time_commit, timespan1);
    this->wait_latch_time = this->wait_latch_time + timespan1;
#endif

    if (rc == Abort){
        abort_process(this);
        return Abort;
    }

//    for(auto & dep_pair :*hotspot_friendly_dependency){
//        if (dep_pair.dep_txn != nullptr){
//            dep_pair.dep_txn->SemaphoreSubOne();
//            dep_pair.dep_type = INVALID; // Making concurrent_vector correct
//        }
//    }
//    hotspot_friendly_dependency->clear();

    /*** Writing phase */
    // hotspot_friendly_serial_id may be updated because of RW dependency.

#if PF_CS
    uint64_t startt_latch = get_sys_clock();
#endif
    for(int rid = 0; rid < row_cnt; rid++){
        accesses[rid]->lock_entry->status = LOCK_DROPPED;
        if (accesses[rid]->type == RD){
            continue;
        }

        accesses[rid]->orig_row->manager->lock_row(this);
        auto new_version = accesses[rid]->tuple_version;
        auto old_version = accesses[rid]->old_version;
        if (old_version->type == AT){
            while (true){
                old_version = old_version->next;
                if (old_version != nullptr && old_version->type != AT){
                    break;
                }
            }
        }
        assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);
        old_version->end_ts = this->rr_serial_id;
        new_version->begin_ts = this->rr_serial_id;
        new_version->retire = nullptr;
        new_version->type = XP;

//        if (this == accesses[rid]->orig_row->manager->owner ){
//            accesses[rid]->orig_row->manager->owner = nullptr;
//        }

        //bring next
//        if (accesses[rid]->orig_row->manager->owner == nullptr || accesses[rid]->orig_row->manager->owner->status == LOCK_RETIRED){
            accesses[rid]->orig_row->manager->bring_next();
//        }

        accesses[rid]->orig_row->manager->unlock_row(this);
    }

#if PF_CS
    uint64_t timespan = get_sys_clock() - startt_latch;
    INC_STATS(this->get_thd_id(), time_get_cs, timespan);
    this->wait_latch_time = this->wait_latch_time + timespan;
#endif
    /*** Releasing Dependency */
    // Update status.
//     while(!ATOM_CAS(status_latch, false, true))
//         PAUSE
//     assert(status == writing);
//    ATOM_CAS(status, writing, committing);
//     status_latch = false;

    // Update status.
//    while(!ATOM_CAS(status_latch, false, true))
//        PAUSE
//    assert(status == committing);
    ATOM_CAS(status, validating, COMMITED);
//    status_latch = false;

    this->i_dependency_on.clear();
    this->rr_dependency->clear();

    return rc;
}

void txn_man::abort_process(txn_man * txn ){

//    uint64_t starttime = get_sys_clock();
#if PF_CS
    uint64_t time_wound = get_sys_clock();
#endif
    if (status == RUNNING || status == validating){
        txn->set_abort();
    }
#if PF_CS
    INC_STATS(txn->get_thd_id(), time_wound, get_sys_clock() - time_wound);
#endif

#ifdef ABORT_OPTIMIZATION
//    if(wr_cnt != 0){
        for(int rid = 0; rid < row_cnt; rid++) {
            accesses[rid]->lock_entry->status = LOCK_DROPPED;
            Version *new_version = (Version *) accesses[rid]->tuple_version;
            if (accesses[rid]->type == RD) {
                continue;
            }

            ATOM_CAS(new_version->type, WR, AT);
        }

#if VERSION_CHAIN_CONTROL
            //4-3 Restrict the length of version chain. [Subtract the count of uncommitted versions.]
            accesses[rid]->orig_row->manager->DecreaseThreshold();
#endif

//            // We record new version in read_write_set.
//            Version* old_version;
//            Version* row_header;
////            assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);
//            row_header = accesses[rid]->orig_row->manager->get_version_header();
//            uint64_t vh_chain = (row_header->version_number & CHAIN_NUMBER) >> 40;
//            uint64_t my_chain = ((new_version->version_number & CHAIN_NUMBER) >> 40) + 1;
//            // [No Latch]: Free directly.
//            if(vh_chain > my_chain){
//                assert(row_header->version_number > new_version->version_number);
//                new_version->retire = nullptr;
////                new_version->retire_ID = 0;
//                new_version->prev = NULL;
////                new_version->next = NULL;
//                new_version->type = AT;
//                //TODO: can we just free this object without reset retire and retire_ID?
////                _mm_free(new_version);
////                new_version = NULL;
//
////                txn->h_thd->free_version(new_version);
//            }
//            else{       // In the same chain.
//                accesses[rid]->orig_row->manager->lock_row(this);
//
//                // update the version_header if there's a new header.
//                row_header = accesses[rid]->orig_row->manager->get_version_header();
//                vh_chain = (row_header->version_number & CHAIN_NUMBER) >> 40;
//                my_chain = ((new_version->version_number & CHAIN_NUMBER) >> 40);
//                assert(vh_chain >= my_chain);
//                // No longer in the same chain. [No Latch]: Free directly.
//                if(vh_chain != my_chain) {
//                    assert(row_header->version_number > new_version->version_number);
//                    accesses[rid]->orig_row->manager->unlock_row(this);
//                    new_version->retire = nullptr;
////                    new_version->retire_ID = 0;
//                    new_version->prev = NULL;
////                    new_version->next = NULL;
//                    new_version->type = AT;
//                    //TODO: can we just free this object without reset retire and retire_ID?
////                    _mm_free(new_version);
////                    new_version = NULL;
//
//                }
//                    // [Need Latch]: We are in the same chain.
//                else{
//                    // Get the old_version and depth of version_header and I.
//                    old_version = new_version->next;
//                    uint64_t vh_depth = (row_header->version_number & DEEP_LENGTH);
//                    uint64_t my_depth = (new_version->version_number & DEEP_LENGTH);
//                    // Check again to avoid acquiring unnecessary latch. [Only need latch when I'm in the front of version_header]
//                    if(vh_depth >= my_depth) {
//                        // new version is the newest version
//                        if (new_version == row_header) {
//                            if (new_version->prev == NULL) {
//                                assert(old_version != NULL);
//                                accesses[rid]->orig_row->manager->version_header = old_version;
//                                assert(accesses[rid]->orig_row->manager->version_header->end_ts == UINT64_MAX);
//
////                                assert(old_version->prev == new_version);
//                                old_version->prev = NULL;
//                                new_version->type = AT;
//
//                                // Recursively update the chain_number of uncommitted old version and the first committed verison.
//                                Version* version_retrieve = accesses[rid]->orig_row->manager->version_header;
//
//                                while(version_retrieve->begin_ts == UINT64_MAX){
////                                    assert(version_retrieve->retire != NULL);
//                                    if (version_retrieve->retire != nullptr){
//                                        version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
//                                    }
//                                    version_retrieve = version_retrieve->next;
//                                }
//
//                                // Update the chain-number of the first committed version.
//                                assert(version_retrieve->begin_ts != UINT64_MAX && version_retrieve->retire == NULL  );
//                                version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
//                            } else {
//                                accesses[rid]->orig_row->manager->version_header = old_version;
//
//                                assert(accesses[rid]->orig_row->manager->version_header->end_ts == INF);
//                                assert(old_version->prev == new_version);
//
//                                // Should link these two versions.
//                                Version *pre_new = new_version->prev;
//                                if (pre_new && pre_new->next == new_version) {
//                                    pre_new->next = old_version;
//                                }
//                                if (old_version->prev == new_version) {
//                                    // Possible: old_version is already pointing to new version.
//                                    old_version->prev = pre_new;
//                                }
//
//                                new_version->prev = NULL;
//                                new_version->type = AT;
//
//                                // Recursively update the chain_number of uncommitted old version and the first committed verison.
//                                Version* version_retrieve = accesses[rid]->orig_row->manager->version_header;
//
//                                while(version_retrieve->begin_ts == UINT64_MAX){
//                                    assert(version_retrieve->retire != NULL);
//                                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
//                                    version_retrieve = version_retrieve->next;
//                                }
//
//                                // Update the chain-number of the first committed version.
//                                assert(version_retrieve->begin_ts != UINT64_MAX && version_retrieve->retire == NULL );
//                                version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
//                            }
//                        } else {
//                            // Should link these two versions.
//                            Version *pre_new = new_version->prev;
//                            if (pre_new && pre_new->next == new_version) {
//                                pre_new->next = old_version;
//                            }
//                            if (old_version->prev == new_version) {
//                                // Possible: old_version is already pointing to new version.
//                                old_version->prev = pre_new;
//                            }
//
//                            new_version->prev = NULL;
//                            new_version->type = AT;
//                        }
//
//                        new_version->retire = nullptr;
////                        new_version->retire_ID = 0;                //11-17
//
//                        //TODO: Notice that begin_ts and end_ts of new_version both equal to MAX
//
////                        _mm_free(new_version);
////                        new_version = NULL;
//
//                        accesses[rid]->orig_row->manager->unlock_row(this);
////                        txn->h_thd->free_version(new_version);
//                    }
//                    else{
//                        // Possible: May be I'm the version_header at first, but when I wait for blatch, someone changes the version_header to a new version.
//                        assert(vh_chain == my_chain && vh_depth < my_depth);
//
//                        accesses[rid]->orig_row->manager->unlock_row(this);
//
//                        new_version->retire = nullptr;
////                        new_version->retire_ID = 0;
//
//                        new_version->prev = NULL;
//                        new_version->type = AT;
//
//                        //TODO: can we just free this object without reset retire and retire_ID?
//
////                        _mm_free(new_version);
////                        new_version = NULL;
////                        txn->h_thd->free_version(new_version);
//                    }
//                }
//            }
//        }
//    }
#else
    if(wr_cnt != 0){
        for(int rid = 0; rid < row_cnt; rid++){
            if(accesses[rid]->type == RD){
                continue;
            }

            while (!ATOM_CAS(accesses[rid]->orig_row->manager->blatch, false, true)){
                PAUSE
            }

            Version* new_version = (Version*)accesses[rid]->tuple_version;
            Version* old_version = new_version->next;

            assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);

            Version* row_header = accesses[rid]->orig_row->manager->get_version_header();

            // new version is the newest version
            if(new_version == row_header) {
                if (new_version->prev == NULL) {
                    accesses[rid]->orig_row->manager->version_header = old_version;
                    assert(accesses[rid]->orig_row->manager->version_header->end_ts == INF);

                    assert(old_version->prev == new_version);
                    old_version->prev = NULL;
                    new_version->next = NULL;
                }
                else{
                    accesses[rid]->orig_row->manager->version_header = old_version;
                    assert(accesses[rid]->orig_row->manager->version_header->end_ts == INF);

                    assert(old_version->prev == new_version);
                    old_version->prev = new_version->prev;
                    new_version->prev->next = old_version;

                    new_version->prev = NULL;
                    new_version->next = NULL;
                }
            }
            else{
                Version* pre_new = new_version->prev;
                if(pre_new){
                    pre_new->next = old_version;
                }
                if(old_version->prev == new_version){
                    // I think (old_version->prev == new_version) is always true.
                    old_version->prev = pre_new;
                }
                new_version->prev = NULL;
                new_version->next = NULL;
            }

            new_version->retire = nullptr;
            new_version->retire_ID = 0;

            //TODO: Notice that begin_ts and end_ts of new_version both equal to MAX

            _mm_free(new_version);
            new_version = NULL;

            accesses[rid]->orig_row->manager->blatch = false;
        }
    }
#endif


    for(auto & dep_pair :*rr_dependency){
        // only inform the txn which wasn't aborted
        if (dep_pair.dep_type == READ_WRITE_){
//            dep_pair.dep_txn->SemaphoreSubOne();
        } else{
            if (dep_pair.dep_txn != nullptr){
                if (dep_pair.dep_txn->status == RUNNING  || dep_pair.dep_txn->status == validating) {
                    dep_pair.dep_txn->set_abort(5);
 
#if PF_CS
                    INC_STATS(this->get_thd_id(), cascading_abort_cnt, 1);
#endif
                }
            }
        }
    }
    rr_dependency->clear();

//    uint64_t endtime = get_sys_clock();
//    INC_STATS(this->get_thd_id(), time_abort_processing, endtime - starttime);


}


#endif


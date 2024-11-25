#pragma once

#include "concurrency_control/row_rr.h"
#include "global.h"

class workload;
class base_query;

class thread_t {
public:
    uint64_t _thd_id;
    workload * _wl;

    uint64_t 	get_thd_id();

    uint64_t 	get_host_cid();
    void 	 	set_host_cid(uint64_t cid);

    uint64_t 	get_cur_cid();
    void 		set_cur_cid(uint64_t cid);

    void 		init(uint64_t thd_id, workload * workload);
    // the following function must be in the form void* (*)(void*)
    // to run with pthread.
    // conversion is done within the function.
    RC 			run();

    // moved from private to global for clv
    ts_t 		get_next_ts();
    ts_t 		get_next_n_ts(int n);

    std::vector<std::string> *wound_entry;
    void   insert_wound(std::string entry){
        wound_entry->push_back(entry);
    }
    bool   insert_hotspots(uint64_t hots){
        auto ret = hotspots->insert(hots);
        if (ret.second){
            return true;
        }else{
            return false;
        }
    }
#if CC_ALG == REBIRTH_RETIRE
    Version *reserve_version(){
        Version * new_version = nullptr;
        auto reser_index = curr_index % total_sz;
        new_version = free_list->at(reser_index);
        curr_index ++;
        return new_version;
    }

//    void free_version(Version *version){
//        free_list->push_back(version);
//        total_sz = total_sz + 1;
//    }
//
//    void free_versions_mem(){
//// 释放 free_list 中的每个 Version 对象的内存
//        for (auto version_reserve : *free_list) {
//            // 释放 version_reserve->dynamic_txn_ts 的内存
//            if (version_reserve->dynamic_txn_ts) {
//                delete version_reserve->dynamic_txn_ts;  // 使用 delete 来释放
//                version_reserve->dynamic_txn_ts = nullptr; // 防止悬挂指针
//            }
//
//            // 释放 version_reserve->data 的内存
//            if (version_reserve->data) {
//                _mm_free(version_reserve->data);  // 使用 _mm_free 来释放
//                version_reserve->data = nullptr; // 防止悬挂指针
//            }
//
//            // 释放 version_reserve 本身的内存
//            _mm_free(version_reserve);  // 使用 _mm_free 来释放
//        }
//
//// 释放 free_list 本身的内存
//        delete free_list;  // 使用 delete 来释放 free_list
//    }

#endif

private:
    uint64_t 	_host_cid;
    uint64_t 	_cur_cid;
    ts_t 		_curr_ts;

    RC	 		runTest(txn_man * txn);
    drand48_data buffer;

    // added for wound wait
    base_query * curr_query;
    ts_t         starttime;

    std::set<uint64_t> *hotspots;

#if CC_ALG == REBIRTH_RETIRE
    std::vector<Version *> *free_list;
    int total_sz = 900000;  //350MB
    int curr_index = 0;
#endif

    // A restart buffer for aborted txns.
    struct AbortBufferEntry	{
        ts_t ready_time;
        base_query * query;
        ts_t starttime;
    };
    AbortBufferEntry * _abort_buffer;
    int _abort_buffer_size;
    int _abort_buffer_empty_slots;
    bool _abort_buffer_enable;
};

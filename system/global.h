#pragma once

#include "stdint.h"
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
//#define NDEBUG
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <mm_malloc.h>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include "tbb/tbb.h"

#if LATCH == LH_MCSLOCK
#include "mcs_spinlock.h"
#endif
#include "pthread.h"
#include "config.h"
#include "stats.h"
#include "dl_detect.h"
#ifndef NOGRAPHITE
#include "carbon_user.h"
#endif
#include "helper.h"

using namespace std;

class mem_alloc;
class Stats;
class DL_detect;
class Manager;
class Query_queue;
class Plock;
class OptCC;
class VLLMan;

typedef uint32_t UInt32;
typedef int32_t SInt32;
typedef uint64_t UInt64;
typedef int64_t SInt64;

typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Data Structure
/******************************************/
extern mem_alloc mem_allocator;
extern Stats stats;
extern DL_detect dl_detector;
extern Manager * glob_manager;
extern Query_queue * query_queue;
extern Plock part_lock_man;
extern OptCC occ_man;
#if CC_ALG == VLL
extern VLLMan vll_man;
#endif

extern bool volatile warmup_finish;
extern bool volatile enable_thread_mem_pool;
extern pthread_barrier_t warmup_bar;
extern pthread_barrier_t stage1_barrier;
extern pthread_barrier_t stage2_barrier;
extern pthread_barrier_t stage3_barrier;
extern int volatile stage_cnt;
extern UInt32 volatile tmp_thd_cnt;
extern uint64_t g_txn_max_part ;
extern UInt32 g_max_tuple_size ;
#ifndef NOGRAPHITE
extern carbon_barrier_t enable_barrier;
#endif

/******************************************/
// Global Parameter
/******************************************/
extern bool g_part_alloc;
extern bool g_mem_pad;
extern bool g_prt_lat_distr;
extern UInt32 g_part_cnt;
extern UInt32 g_virtual_part_cnt;
extern UInt32 g_thread_cnt;
extern ts_t g_abort_penalty;
extern bool g_central_man;
extern UInt32 g_ts_alloc;
extern bool g_key_order;
extern bool g_no_dl;
extern ts_t g_timeout;
extern ts_t g_dl_loop_detect;
extern bool g_ts_batch_alloc;
extern UInt32 g_ts_batch_num;

extern map<string, string> g_params;

// YCSB
extern UInt32 g_cc_alg;
extern ts_t g_query_intvl;
extern UInt32 g_part_per_txn;
extern double g_perc_multi_part;
extern double g_read_perc;
extern double g_write_perc;
extern double g_zipf_theta;
extern UInt64 g_synth_table_size;
extern UInt32 g_req_per_query;
extern UInt32 g_field_per_tuple;
extern UInt32 g_init_parallelism;
extern double g_last_retire;
extern double g_specified_ratio;
extern double g_flip_ratio;
extern double g_long_txn_ratio;
extern double g_long_txn_read_ratio;

// TPCC
extern UInt32 g_num_wh;
extern double g_perc_payment;
extern double g_perc_query_2;
extern double g_perc_delivery;
extern double g_perc_orderstatus;
extern double g_perc_stocklevel;
extern double g_perc_neworder;
extern bool g_wh_update;
extern char * output_file;
extern UInt32 g_max_items;
extern UInt32 g_cust_per_dist;
extern uint64_t g_max_orderline;

extern UInt32 g_stat_processing0;
extern UInt32 g_stat_processing1;
extern UInt32 g_stat_processing2;
extern UInt32 g_stat_processing3;
extern UInt32 g_stat_processing4;
extern UInt32 g_stat_processingc;
extern UInt32 g_stat_processinguc;
extern UInt32 g_stat_processing5_ABORTED;
extern UInt32 g_stat_processing5_Abort;
extern UInt32 g_stat_processing6;
extern UInt32 g_stat_processing7;
extern UInt32 g_stat_processingn_abort;
extern UInt32 g_stat_processingp_abort;
extern UInt32 g_stat_processing_set_ABORTED1;
extern UInt32 g_stat_processing_set_ABORTED2;
extern UInt32 g_stat_processing_set_ABORTED3ww;
extern UInt32 g_stat_processing_set_ABORTED3wr;
extern UInt32 g_stat_processing_set_abort_call;

enum RC { RCOK, Commit, Abort, WAIT, ERROR, FINISH};

/* Thread */
typedef uint64_t txnid_t;

/* Txn */
typedef uint64_t txn_t;

/* Table and Row */
typedef uint64_t rid_t; // row id
typedef uint64_t pgid_t; // page id



/* INDEX */
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {INDEX_INSERT, INDEX_READ, INDEX_NONE};
typedef uint64_t idx_key_t; // key id for index
typedef uint64_t (*func_ptr)(idx_key_t);	// part_id func_ptr(index_key);

/* general concurrency control */
enum access_t {RD, WR, XP, SCAN, CM, AT};
/* LOCK */
enum lock_t {LOCK_EX, LOCK_SH, LOCK_NONE };
enum loc_t {RETIRED, OWNERS, WAITERS, LOC_NONE};
enum lock_status {LOCK_DROPPED, LOCK_WAITER, LOCK_OWNER, LOCK_RETIRED};
/* TIMESTAMP */
enum TsType {R_REQ, W_REQ, P_REQ, XP_REQ};
/* TXN STATUS */
// XXX(zhihan): bamboo requires the enumeration order to be unchanged
// HOTSPOT_FRIENDLY: validating[enter validate phase], writing[enter write phase], committing[release dependency]
//enum status_t: unsigned int {RUNNING, ABORTED, COMMITED, HOLDING, validating, writing, committing};
enum status_t: unsigned int {RUNNING, ABORTED, COMMITED,  validating, writing, committing};

/* COMMUTATIVE OPERATIONS */
enum com_t {COM_INC, COM_DEC, COM_NONE};

/* HOTSPOT_FRIENDLY:dependency type */
enum DepType {
    INVALID = 0,
    READ_WRITE_ = 1,  /* Binary: 001*/
    WRITE_READ_ = 2,  /* Binary: 010*/
    WRITE_WRITE_ = 4,  /* Binary: 100*/
//    RW_WR = 3,  /* Binary: 011*/
//    RW_WW = 5,  /* Binary: 101*/
//    WR_WW = 6,  /* Binary: 110*/
//    RW_WR_WW = 7,  /* Binary: 111*/
    WRONG = 100,
    WRONG_1 = 200,

    READ_READ_
};

//curr_txn_id, dep_txn_id, dep_type
//for test
typedef std::pair<uint64_t,std::pair<uint64_t,DepType>> dependent;
extern tbb::concurrent_vector<dependent> depen_element_print ;
extern tbb::concurrent_vector<std::pair<uint64_t,uint64_t>> abort_txns_print ;
extern std::set<uint64_t> write_keys;
extern std::vector<uint64_t> ww_txns;
extern tbb::concurrent_vector<std::pair<string, std::pair<string,std::pair<string, std::pair<string, string>>>>> wound_retired_wr_list ;
extern tbb::concurrent_vector<std::pair<string, std::pair<string,string>>> wound_retired_rd_list ;
extern tbb::concurrent_vector<std::pair<string,  string>> wound_owners_list ;
extern std::atomic<uint64_t> wound_retire_count;
extern std::atomic<uint64_t> wound_owner_count;
extern  std::atomic<uint64_t> one_hot_txn_count;

/* HOTSPOT_FRIENDLY: return type of pushDependency() */
//enum bool_dep{
//    NOT_CONTAIN = 0,
//    CONTAIN_TXN = 1,
//    CONTAIN_TXN_AND_TYPE = 2
//};

/* HOTSPOT_FRIENDLY: addable write_set */
/*
struct write_set_element {
    //需要增加一个无参数的构造函数，后续使用 write_set_element 来声明一个新的变量时，会用到这个构造函数的
    write_set_element() :
    tuple_version(ItemPointer()),
    operation_type(RWType::INVALID) {}

    write_set_element(const ItemPointer tuple_version, const RWType operation_type) :
    tuple_version(tuple_version),
    operation_type(operation_type) {}

    ItemPointer tuple_version;
    RWType operation_type;

    //  std::queue<TransactionContext*> dependencies;

    //访问过当前事务修改后的 tuple version 的事务，使用 map 是为了方便判断，这个事务已经访问过这个 tuple version 了
    tbb::concurrent_unordered_map<uint64_t, TransactionContext*>  dependencies;
};

typedef tbb::concurrent_unordered_map<uint64_t, write_set_element>  WriteSet_Mix;
*/

//4-3 Restrict the length of version chain.[Unused]
//extern uint64_t version_chain_threshold;


#define MSG(str, args...) { \
	printf("[%s : %d] " str, __FILE__, __LINE__, args); } \
//	printf(args); }

// principal index structure. The workload may decide to use a different 
// index structure for specific purposes. (e.g. non-primary key access should use hash)
#if (INDEX_STRUCT == IDX_BTREE)
#define INDEX		index_btree
#else  // IDX_HASH
#define INDEX		IndexHash
#endif

/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX 		18446744073709551615UL
#endif // UINT64_MAX


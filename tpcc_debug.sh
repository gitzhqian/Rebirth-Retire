cp -r config_tpcc_debug.h config.h
rm temp.out
rm debug.out

wl="TPCC"
threads=16
cnt=100000
penalty=0 #1
wh=1
spin="true"
pf="true"
alg="CLV"
on=10
off=17
phs="true"
phs="false"
dynamic="true"
dynamic="false"
debug="false"
#debug="true"
bench="false"
#bench="true"
#perc=0.5
perc=1
#perc=1
merge="true"
merge="false"
reorder="false"
#reorder="true"
retire="true"
#retire="false"



for alg in CLV #WOUND_WAIT #WAIT_DIE NO_WAIT
do
#alg="WOUND_WAIT"
timeout 90 python test.py RETIRE_ON=$retire REORDER_WH=$reorder MERGE_HS=$merge PERC_PAYMENT=$perc DEBUG_BENCHMARK=$bench DEBUG_CLV=$debug DYNAMIC_TS=$dynamic PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on CLV_RETIRE_OFF=$off DEBUG_PROFILING=$pf SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out
done


#timeout 50 python test.py DEBUG_TMP="false" PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on CLV_RETIRE_OFF=$off DEBUG_PROFILING=$pf SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out

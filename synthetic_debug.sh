cp -r config_ycsb_synthetic.h config.h
rm debug.out

wl="YCSB"
alg=WAIT_DIE
threads=64
cnt=100000
penalty=50000
zipf=0
synthetic=true
table_size="1024*1024*20"
profile="true"
req=64
spin="true"
on=1
dynamic="true"
hs=2
pos=TM
fhs="WR"
shs="WR"
read_ratio=1
phs="true"
phs="false"
think=0


timeout 5000 python test.py THINKING_TIME=$think PRIORITIZE_HS=$phs READ_PERC=1 NUM_HS=$hs FIRST_HS=$fhs POS_HS=$pos DEBUG_TMP="false" DYNAMIC_TS=$dynamic CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
#phs="false"

#timeout 30 python test.py PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out

cd ../
cp -r config-ycsb-std.h config.h

# algorithm
alg=BAMBOO
latch=LH_MCSLOCK
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire_on="true"
cs_pf="false"
opt_raw="true"

# workload
wl="YCSB"
req=16
synthetic=false
zipf=0
num_hs=0
pos=TOP
specified=0
fixed=1
fhs="WR"
shs="WR"
read_ratio=0.5
ordered="false"
flip=0
table_size="10000000"

# other
threads=16
profile="true"
cnt=100000
penalty=50000

for i in 0 1 2 3 4
do
for threads in 1 2 4 8 16 32
do
for dynamic in true false
do
for zipf in 0.5 0.7 0.9 0.99
do
timeout 100 python test.py CC_ALG=${alg} LATCH=${latch} WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=${dynamic} RETIRE_ON=${retire_on} DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} WORKLOAD=${wl} REQ_PER_QUERY=$req SYNTHETIC_YCSB=$synthetic ZIPF_THETA=$zipf NUM_HS=${num_hs} POS_HS=$pos SPECIFIED_RATIO=${specified} FIXED_HS=${fixed} FIRST_HS=$fhs SECOND_HS=$shs READ_PERC=${read_ratio} KEY_ORDER=$ordered FLIP_RATIO=${flip} SYNTH_TABLE_SIZE=${table_size} THREAD_CNT=$threads DEBUG_PROFILING=$profile MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty
done
done
done
done

cd outputs/
python3 collect_stats.py
mv stats.csv ycsb_bb.csv
mv stats-summarized.csv ycsb_bb-summarized.csv
mv stats.json ycsb_bb.json
cd ..

cd experiments
python3 send_email.py ycsb
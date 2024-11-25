#! /bin/bash


zip_theta=(0 0.5 0.7 0.8 0.9 0.99)
read_ratio=(0.1 0.3 0.5 0.7 0.9)
write_ratio=(0.9 0.7 0.5 0.3 0.1)

sed -i '21s/.*/#define  WORKLOAD 					    YCSB/g' config.h
# sed -i '147s/.*/#define SYNTH_TABLE_SIZE 			10000000/g' config.h
# sed -i '149s/.*/#define READ_PERC 					  0.5/g' config.h
# sed -i '150s/.*/#define WRITE_PERC 					  0.5/g' config.h
# sed -i '159s/.*/#define REQ_PER_QUERY				  16/g' config.h
# sed -i '139s/.*/#define MAX_TXN_PER_PART 			100000/g' config.h
sed -i '160s/.*/#define LONG_TXN_RATIO              0/g' config.h
sed -i '164s/.*/#define SYNTHETIC_YCSB              false/g' config.h

sed -i '60s/.*/#define ABORT_BUFFER_SIZE           1/g' config.h
sed -i '62s/.*/#define ABORT_BUFFER_ENABLE			   true/g' config.h
sed -i '57s/.*/#define ABORT_PENALTY               5000/g' config.h


## vary zif theta
# WOUND_WAIT  BAMBOO   REBIRTH_RETIRE DL_DETECT  SILO
zip_theta=(0.95)
thd=(1 10 20 30 40)
oper=(1 2 4 8 16 32)
dd=(10000 100000 1000000 5000000 10000000)
read_ratio=(0 0.2 0.5 0.8 1)
write_ratio=(1 0.8 0.5 0.2 0)
cc_name=(BAMBOO)

sed -i '8s/.*/#define   THREAD_CNT                       10/g'  config.h
sed -i '148s/.*/#define ZIPF_THETA 					             0/g' config.h
sed -i '149s/.*/#define READ_PERC 					             0.9/g' config.h
sed -i '150s/.*/#define WRITE_PERC 					             0.1/g' config.h
sed -i '147s/.*/#define SYNTH_TABLE_SIZE 			           1000000/g' config.h
sed -i '159s/.*/#define REQ_PER_QUERY				             16/g'     config.h
sed -i '373s/.*/#define NEXT_TS				                   false/g' config.h
sed -i '51s/.*/#define  KEY_ORDER       			           false/g' config.h

sed -i '141s/.*/#define MAX_TUPLE_SIZE                   10/g'  config.h
sed -i '74s/.*/#define  TIMEOUT				                   100000/g' config.h
sed -i '139s/.*/#define MAX_TXN_PER_PART 			           100000/g' config.h
sed -i '316s/.*/#define TEST_TARGET                      tmptest/g' config.h
sed -i '109s/.*/#define BB_LAST_RETIRE                   0.15/g' config.h
sed -i '365s/.*/#define BB_TRACK_DEPENDENS				       false/g' config.h
sed -i '371s/.*/#define WOUND_TEST				               false/g' config.h
sed -i '372s/.*/#define RETIRE_TEST				               false/g' config.h
sed -i '264s/.*/#define PF_ABORT				                 true/g' config.h
sed -i '374s/.*/#define ADAPTIVE_CC				               false/g' config.h
sed -i '376s/.*/#define RBTHREOLD				                 0.01/g' config.h
sed -i '375s/.*/#define REBIRTH				                   true/g' config.h

# sed -i '159s/.*/#define REQ_PER_QUERY				  '${oper[$i]}'/g' config.h
# sed -i '8s/.*/#define THREAD_CNT					    '${thd[$i]}'/g' config.h
# sed -i '148s/.*/#define ZIPF_THETA 					  '${zip_theta[$i]}'/g' config.h
# sed -i '147s/.*/#define SYNTH_TABLE_SIZE 			'${dd[$i]}'/g' config.h
# sed -i '149s/.*/#define READ_PERC 					'${read_ratio[$i]}'/g' config.h
# sed -i '150s/.*/#define WRITE_PERC 					'${write_ratio[$i]}'/g' config.h

for((a=0;a<${#cc_name[@]};a++))
do
  sed -i '44s/.*/#define CC_ALG 					      	'${cc_name[$a]}'/g' config.h

  for((i=0;i<1;i++))
  do
    # sed -i '149s/.*/#define READ_PERC 					'${read_ratio[$i]}'/g' config.h
    # sed -i '150s/.*/#define WRITE_PERC 					'${write_ratio[$i]}'/g' config.h

    make clean
    make -j
    ./rundb
  done
done


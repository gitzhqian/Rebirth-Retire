DBx1000 Rebirth-Retire
==============
The repository is built on DBx1000: 

https://github.com/yxymit/DBx1000 
```
     Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores
     Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker
     http://www.vldb.org/pvldb/vol8/p209-yu.pdf
```

https://github.com/ScarletGuo/Bamboo-Public
```
    Releasing Locks As Early As You Can: Reducing Contention of Hotspots by Violating Two-Phase Locking
    Zhihan Guo, Kan Wu, Cong Yan, Xiangyao Yu
```

The major changes made in this repository:
- added support for Rebirth-Retire and its optimizations. Rebirth-Retire is a concurrency control protocol proposed in:
```
    Rebirth-Retire: A Concurrency Control Protocol Adaptable to Different Levels of Contention
    Qian Zhang, Jianhao Wei, Yifan Li, Xueqing Gong
```
- focused on support for: DL_DETECT, WOUND_WAIT, BamBoo, SILO, Rebirth-Retire
- updated runtime statistics to enable more detailed analysis of the time spent on different tasks during transaction execution
- modified test scripts to facilitate easier evaluation and assessment


Build & Test
------------

To evaluate the database, the following script can be executed, 

    bash ycsb-test.sh

and the resulting outputs are as follows:
```
[summary] throughput=186580, abort_rate=0.64562, txn_cnt= 3972808, abort_cnt= 7237790, user_abort_cnt= 0, abort_cnt_neworder= 0, abort_cnt_payment= 0, run_time= 851.713, time_abort= 455.187, time_cleanup= 126.759, time_lockrow= 0, time_exec= 0, time_assign= 0, time_rebirth= 0, time_creat_entry= 0.000602363, time_creat_version= 0, time_read_write= 4.05957, time_verify= 0, time_wound= 0.925127, time_wound_cascad= 0, abort_position= 0, abort_position_cnt= 0, abort_hotspot= 0, time_abort_processing= 0, time_commit_processing= 0, time_query= 419.796, time_get_latch= 70.8185, time_get_cs= 25.05, time_copy_latch= 16.8747, time_retire_latch= 32.1449, time_retire_cs= 20.2863, time_release_latch= 37.4014, time_release_cs= 18.7515, time_semaphore_cs= 1.64631, time_commit= 59.8887, total_read_wait= 0, total_write_wait= 0, time_ts_alloc= 0, wait_cnt= 4579529, latency= 214462, commit_latency= 87442, abort_length= 56889728, cascading_abort_times= 53538939, max_abort_length= 20, find_circle_abort_depent= 0, txn_cnt_long= 0, abort_cnt_long= 0, cascading_abort_cnt= 118673678, lock_acquire_cnt= 133031222, lock_directly_cnt= 115243079, blind_kill_count= 0, time_wait= 38.8667, time_man= 132.699, time_index= 3.34651, time_copy= 38.6654, deadlock_cnt=0, cycle_detect=0, dl_detect_time=0, dl_wait_time=0
```

The following figure presents the results of  the evaluated protocols under the YCSB workloads:
![image](https://github.com/user-attachments/assets/5ea6d5f3-f692-4004-9c11-d5bd4002079e)
    
Configure & Run
---------------

Supported configuration parameters can be found in config-std.h file. Extra configuration parameters include: 
```
    UNSET_NUMA        : default is false. If set false, it will disable numa effect by interleavingly allocate data. 
    NDEBUG            : default is true. If set true, it will disable all assert()
    COMPILE_ONLY      : defalut is false. If set false, it will compile first and then automatically execute. 
```
Options to change/pass configuration:
- Option 1: use basic configurations provided in ycsb-test.sh. overwrite existing configurations. 
- Option 2: directly copy config-std.h to config.h and modify config.h. Then compile using ```make -j``` and execute through ```./rundb ```









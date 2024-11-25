DBx1000 Rebirth-Retire
==============
The repository is built on DBx1000: 
     https://github.com/yxymit/DBx1000 
     https://github.com/ScarletGuo/Bamboo-Public
    
    Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores
    Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker
    http://www.vldb.org/pvldb/vol8/p209-yu.pdf
    Releasing Locks As Early As You Can: Reducing Contention of Hotspots by Violating Two-Phase Locking
    Zhihan Guo, Kan Wu, Cong Yan, Xiangyao Yu
 
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

To test the database

    bash ycsb-test.sh

    
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









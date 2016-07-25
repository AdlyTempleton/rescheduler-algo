set +x
nohup ./rescheduler-algo.exe -background=true -computeOptimal=true -threshold=0 -nodeSize=.$1 > out/with_background_0_small$1.log &
nohup ./rescheduler-algo.exe -background=true -computeOptimal=true -threshold=1 -nodeSize=.$1 > out/with_background_1_small$1.log &
nohup ./rescheduler-algo.exe -background=true -computeOptimal=true -threshold=1.5 -nodeSize=.$1 > out/with_background_15_small$1.log &
nohup ./rescheduler-algo.exe -background=true -computeOptimal=true -preemptive=true -threshold=1 -nodeSize=.$1 > out/with_both_1_small$1.log &
nohup ./rescheduler-algo.exe -computeOptimal=true -preemptive=true -nodeSize=.$1 > out/with_preemptive_small$1.log &
nohup ./rescheduler-algo.exe -computeOptimal=true -nodeSize=.$1 > out/with_none_small$1.log &
nohup ./rescheduler-algo -background=true -computeOptimal=true -threshold=0 > out/with_background_0.log &
nohup ./rescheduler-algo -background=true -computeOptimal=true -threshold=1 > out/with_background_1.log &
nohup ./rescheduler-algo -background=true -computeOptimal=true -threshold=1.5 > out/with_background_15.log &
nohup ./rescheduler-algo -background=true -computeOptimal=true -preemptive=true -threshold=1 > out/with_both_1.log &
nohup ./rescheduler-algo -computeOptimal=true -preemptive=true > out/with_preemptive.log &
nohup ./rescheduler-algo -computeOptimal=true > out/with_none.log &
# rescheduler-algo
This program simulates possible modifications of a cluster scheduler. It is primarily created to explore possible rescheduler algorithms for use in Kubernetes

#Method
Data used from https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md (though other similarily-formatted sources can be used)

This program simulates the packing of tasks onto nodes in a cluster. Nodes contain some amount of CPU and Memory resources, as well as a list of present tasks.

Each task requires some amount of CPU/Memory resources on the node on which it is scheduled. Each talk also contains a start and end time. The time procedes through a month-long period, adding and removing tasks accordingly.

Newly added tasks are scheduled onto a node in the cluster according to some algorithm. By default, this algorithm is a rough emulation of Kubernete's scheduling algorithm. If a task cannot be scheduled, the cluster is automatically upscaled.

A rescheduler can also be used. This rescheduler can either move tasks in order than pending tasks might schedule or move tasks in the background.

#Output
Relavent output of the program, in various formats, is present in `/out`

#Data
Due to space constraints, the input data is not present in this repository. To recreate this data, the following steps must be followed

task_events must be downloaded from https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md, unzipped and combined into a single file

The file should then be sorted by the following command

`sort -t',' -k3,4R -k1,1n task_events.csv`

package main

import (
	"fmt"
	"flag"
	"bufio"
	"encoding/csv"
	"os"
	"io"
	"strconv"
	"math"
	"sort"
	"log"
	"runtime/pprof"
)

type Task struct {
	mem          float64
	cores        float64

	priority     int
	jobID        int

	start        int
	end          int

}

//ByStartTime allows a []*Task to be sorted by the start time of the tasks
type ByStartTime []*Task

func (tasks ByStartTime) Len() int{
	return len(tasks)
}

func (tasks ByStartTime) Swap(i, j int){
	tasks[i], tasks[j] = tasks[j], tasks[i]

}


func (tasks ByStartTime) Less(i, j int) bool {
	return tasks[i].start < tasks[j].start
}

//BySize allows a []*Task to be sorted by the maximum resource size
type BySize []*Task

func (tasks BySize) Len() int{
	return len(tasks)
}

func (tasks BySize) Swap(i, j int){
	tasks[i], tasks[j] = tasks[j], tasks[i]
}

func (tasks BySize) Less(i, j int) bool {
	iMax, jMax := math.Max(tasks[i].cores, tasks[i].mem), math.Max(tasks[j].cores, tasks[j].mem)
	return iMax < jMax
}

//lastJobID keeps track of the id of the most recently created job.
var lastJobID int = 1

//Node represents a single node of a cluster. Keeps track of it's available resources, as well as the tasks residing on it
type Node struct {
	name string
	tasks []*Task
	cores float64
	mem   float64
}

//usedResources sums the resource requirements of all tasks on the node
func (node *Node) usedResources()(cores float64, mem float64){
	cores, mem = 0, 0

	for _, task := range node.tasks{
		cores, mem = cores + task.cores, mem + task.mem
	}
	return
}

//freeResources computes the resources available on a node for scheduling more tasks
func (node *Node) freeResources()(cores float64, mem float64){
	usedCores, usedMemory := node.usedResources()
	return node.cores - usedCores, node.mem - usedMemory
}

//couldFit determines whether a task could fit on a node
func (node *Node) couldFit(task *Task)(bool){
	freeCores, freeMem := node.freeResources()
	return freeMem >= task.mem && freeCores >= task.cores
}

//removeTask deletes a task from a node
func (node *Node) removeTask(i int)(task *Task){
	task = node.tasks[i]
	node.tasks = append(node.tasks[:i], node.tasks[i + 1:]...)
	return
}

//clone creates a clone of this node, including a shallow clone of tasks
func (node *Node) clone()(*Node){
	tmp := make([]*Task, len(node.tasks))
	copy(tmp, node.tasks)
	return &Node{node.name, tmp, node.cores, node.mem}
}

//getUtilization returns two floats which represent the percentage utilization of cpu and memory, respectively
func (node *Node) getUtilization()(float64, float64){

	//Take the percentages
	cores, mem := node.usedResources()
	cores = cores / node.cores
	mem = mem / node.mem
	return cores, mem
}

//Scheduler represents an interface to implement different scheduling policies
type Scheduler interface {
	//schedule schedules a task on a cluster
	schedule(task *Task, cluster []*Node) (node *Node, priority int, err error)
}

//Rescheduler represents an interface to implement different rescheduling policies
type Rescheduler interface {
	//Attempts to schedule a task, possibly by preemptive and moving other tasks
	rescheduleFailedTask(task *Task, cluster []*Node, time int) (err error)

	//poll moves any or all tasks to different nodes according to some algorithm
	//The returned value is taken as the new state of the cluster
	poll(time int, cluster []*Node)([]*Node)
}

//newTask is a helper method for creating a new Task which assigns an id automatically
func newTask(mem float64, cores float64, priority int, start int, end int) *Task{
	jobID := lastJobID
	lastJobID ++
	if lastJobID % 100000 == 0{
		fmt.Printf("Initialized Job %d\n", lastJobID)
	}

	return &Task{mem, cores, priority, jobID, start, end}
}

//realData loads a sorted list of Tasks from a file
func realData(filename string, taskCount int) ([]*Task){
	var tasks ByStartTime
	var currentTask *Task

	file, _ := os.Open(filename)
	defer file.Close()

	r := csv.NewReader(bufio.NewReader(file))

	//Load the first n tasks only
	for len(tasks) < taskCount{
		record, err := r.Read()

		if err == io.EOF {
			break
		}

		//If there is an error parsing eventType, we won't take any action on this row
		eventType, _ := strconv.Atoi(record[5])

		if eventType == 0{
			//If a task is pending in currentTask, it continues past the trace
			//And is already added to tasks
			mem, _ := strconv.ParseFloat(record[9], 64)
			cpu, _ := strconv.ParseFloat(record[10], 64)

			//Account for malformed data with resources values over 100%
			mem = math.Min(mem, 1)
			cpu = math.Min(cpu, 1)

			startTime, _ := strconv.Atoi(record[0])

			//Convert microseconds into seconds
			startTime = startTime / 1000000
			priority, _ := strconv.Atoi(record[8])
			currentTask = newTask(mem, cpu, priority, startTime, -1)

			tasks = append(tasks, currentTask)
		}

		//Special code is needed to handle evictions.
		//Evictions are a result of the scheduling algorithm, and so they should not be counted as an interruption
		//However, disregarding eviction events completely is problematic for several reasons:
		//It creates duplicate representation of the same tasks
		//The original representation is an 'orphan': it does not die when the actual task finishes/is killed
		//Significantly increasing the frequency of never-ending tasks
		//Instead, we skip not only the EVICT event but also the subsequent ADD and SCHEDULE events
		//So that the following termination events are applied to the correct task
		if eventType == 2{
			_, _ = r.Read()
			_, _ = r.Read()
		}

		//Range of values which signal terminations/completion
		//Note that we do not consider EVICT (eventType = 2) as those are a result, not an input, of the scheduling algorithm
		//The task is already in tasks, we just need to modify end
		if eventType >= 3 && eventType <= 6{

			//Convert microseconds into seconds
			endTime, _ := strconv.Atoi(record[0])
			endTime = endTime / 1000000
			currentTask.end = endTime
		}
	}

	sort.Sort(tasks)
	return tasks
}


//Returns a collection of many standard nodes
func standardNodeCluster(count int) (cluster []*Node){
	for i := 0; i < count; i++{
		cluster = append(cluster, standardNode())
	}
	return
}

//standardNode returns a standardized node with resource values equal to Google's largest node
func standardNode() *Node{
	nodeIndex++
	return &Node{strconv.Itoa(nodeIndex), nil, nodeSize, nodeSize}
}

//nodeIndex keeps track of the id of the most recently created Node
var nodeIndex int = 0

//preemptiveRescheduling controls the use of the preemptive rescheduler
var preemptiveRescheduling bool

//backgroundRescheduling controls the use of the background rescheduler
var backgroundRescheduling bool

//backgroundReschedulingThreshold controls the aggressiveness of the background rescheduler
var backgroundReschedulingThreshold float64

//computeOptimal controls the calculation of the optimal fit for the number of nodes required to offline schedule the task load
var computeOptimal bool

var downscaling bool

//tolerance represents the consecutive time period a node must be pending for in order to upscale
var tolerance int64

//The initial cluster size
var clusterSize int64

//nodeSize represents the normalized size of all nodes
var nodeSize float64

//Main represents the main simulation loop
func main() {

	//Load command line arguments
	preemptiveReschedulingPtr := flag.Bool("preemptive", false, "Use preemptive scheduling")
	backgroundReschedulingPtr := flag.Bool("background", false, "Use background rescheduler")
	downscalingPtr := flag.Bool("downscale", false, "Use downscaler")
	taskCountPtr := flag.Int("taskCount", 1000000, "The number of tasks to load")
	backgroundReschedulingThresholdPtr := flag.Float64("threshold", 1, "Background Threshold")
	filenamePtr := flag.String("file", "data/task_events_sorted.csv", "Filename to load data from")
	computeOptimalPtr := flag.Bool("computeOptimal", false, "Compute nodes required to offline schedule a task load")
	tolerancePtr := flag.Int64("tolerance", 43200, "Time required before upscaling. Guards against short spikes")
	clusterSizePtr := flag.Int("clusterSize", 100, "Initial size of the cluster")
	nodeSizePtr := flag.Float64("nodeSize", 1, "Normalized size of all nodes")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()

	preemptiveRescheduling = *preemptiveReschedulingPtr
	backgroundRescheduling = *backgroundReschedulingPtr
	backgroundReschedulingThreshold = *backgroundReschedulingThresholdPtr
	downscaling = *downscalingPtr
	tolerance = *tolerancePtr
	computeOptimal = *computeOptimalPtr
	clusterSize := *clusterSizePtr
	nodeSize := *nodeSizePtr

	//Handle profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	time := 0

	//The list of tasks to scheduler
	var schedulerQueue []*Task

	//The list of tasks which will be scheduled in the future
	//This list is assumed to be sorted by start times
	var futureTaskList []*Task
	//Load data from file
	futureTaskList = realData(*filenamePtr, *taskCountPtr)

	//Initialize an auto-scaling cluster
	var cluster []*Node
	cluster = standardNodeCluster(clusterSize)

	//Use the appropriate scheduling and rescheduling algorithms
	var scheduler Scheduler = K8sScheduler{}
	var rescheduler Rescheduler = basicRescheduler{}
	var downscaler Rescheduler = basicDownscaler{}

	//Keep track of the time for while tasks have failed to schedule
	var pendingQueueTime int64 = 0

	//Main simulation loop
	for true{
		//Check for tasks which may be added into the queue
		for true{
			if len(futureTaskList) == 0{
				break
			}

			//Remember that futureTaskList is sorted by start time
			//So that we can stop at the first task scheduled in the future
			if nextTask := futureTaskList[0]; nextTask.start <= time{
				futureTaskList = futureTaskList[1:]
				schedulerQueue = append(schedulerQueue, nextTask)
			}else{
				break
			}
		}

		//Check for tasks which have reached the end of their lifetime and kill them
		for _, node := range cluster{
			//We need to be able to modify a slice while iterating over it
			//This method is preferred to copy-and-replace as deletions are expected to be small
			deleted := 0
			for i := range node.tasks{
				//Shift the index to compensate for deleted tasks
				task := node.tasks[i - deleted]
				if task.end != -1 && task.end <= time{
					_ = node.removeTask(i - deleted)
					deleted++
				}
			}
		}

		//Schedule all tasks in the queue
		//Failed tasks go back into the queue

		//Procede through the queue and schedule each task

		areNodesPending := false
		for i, task := range schedulerQueue {
			//Initial attempt at standard scheduling
			var node *Node
			node, _, err := scheduler.schedule(task, cluster)
			if err != nil{
				//Attempt to preemptively schedule
				if preemptiveRescheduling {
					err = rescheduler.rescheduleFailedTask(task, cluster, time)
				}

				//Return the failed task to the queue and upscale the cluster
				if err != nil{
					pendingQueueTime += 300
					areNodesPending = true
					schedulerQueue = schedulerQueue[i:]

					if pendingQueueTime > tolerance{
						//Upscale
						cluster = append(cluster, standardNodeCluster(10)...)

						cpuUtilization, memUtilization := getTotalUtilization(cluster)
						fmt.Printf("Up,%d,%f,%f,%d\n", len(cluster), cpuUtilization, memUtilization, time)
					}
					break
				}

			}else {
				node.tasks = append(node.tasks, task)
			}
		}

		//If we made it through the queue, reset pendingQueueTime
		if !areNodesPending{
			schedulerQueue = []*Task{}
			pendingQueueTime = 0
		}


		//We increment time in increments of 5 minutes for performance's sake
		time = time + 300



		//Activate background rescheduling every hour
		if backgroundRescheduling && time % 3600 == 0{
			cluster = rescheduler.poll(time, cluster)
		}

		if computeOptimal{
			fmt.Printf("FFD,%d,%d\n", findOptimalFit(cluster), time)
		}


		cpuTotal, memTotal := getTotalResourceUsage(cluster)
		fmt.Printf("TotalResources,%f,%f,%d\n", cpuTotal, memTotal, time)

		if downscaling {
			cluster = downscaler.poll(time, cluster)
		}

		//Report utilization percentages
		cpuUtilization, memUtilization := getTotalUtilization(cluster)
		fmt.Printf("Util,%d,%f,%f\n", time, cpuUtilization, memUtilization)

		//End the simulation after a month
		if time > 3000000{
			for _, node := range cluster{
				nodeCPU, nodeMem := node.getUtilization()
				fmt.Printf("NodeAtEnd,%s,%f,%f,%d\n", node.name, nodeCPU, nodeMem, len(node.tasks))
			}
			break
		}
	}
}

//findOptimalFit attempts to simulate an offline rescheduling of all tasks on a cluster
//We use the offline bin-packing algorithm First Fit Decreasing (FFD), described by Garey et al. 1976
//An extension of the standard first-fit-decreasing algorithm to multidimensional vector packing
//This has a worst-case ratio of somewhere between 2 1/6 - 2 1/3 of the optimal packing
func findOptimalFit(cluster []*Node)(binsNeeded int){
	//Extract all tasks from the cluster
	var tasks BySize

	for _, node := range cluster{
		for _, clusterTask := range node.tasks{
			tasks = append(tasks, clusterTask)
		}
	}
	sort.Sort(sort.Reverse(tasks))

	var newCluster []*Node
	newCluster = append(newCluster, standardNode())

	//Create a new cluster, increasing as needed
	outer:
	for _, task := range tasks{
		for _, node := range newCluster{
			if node.couldFit(task){
				node.tasks = append(node.tasks, task)
				continue outer
			}
		}

		//If no nodes can schedule this task, add another bin
		newNode := standardNode()
		newNode.tasks = append(newNode.tasks, task)
		newCluster = append(newCluster, newNode)
	}

	return len(newCluster)
}

//getTotalUtilization calculates the utilization of the cluster in percentages
func getTotalUtilization(cluster []*Node)(float64, float64){

	cpuUtilization, memUtilization := getTotalResourceUsage(cluster)

	cpuUtilization = cpuUtilization / float64(len(cluster))
	memUtilization = memUtilization / float64(len(cluster))

	return cpuUtilization, memUtilization
}

//getTotalResourceUsage returns the total sum of all resources across all tasks
func getTotalResourceUsage(cluster []*Node)(float64, float64){

	var cpuUtilization, memUtilization float64

	for _, node := range cluster{
		cpuUtilizationDelta, memUtilizationDelta := node.getUtilization()
		cpuUtilization, memUtilization = cpuUtilization + cpuUtilizationDelta * node.cores, memUtilization + memUtilizationDelta * node.mem
	}

	return cpuUtilization, memUtilization

}
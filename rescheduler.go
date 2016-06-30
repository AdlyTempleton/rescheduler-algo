package main

import (
	"math"
	"errors"
	"fmt"
)

type basicRescheduler struct{}

//poll moves tasks according to the normal scheduling policies.
func (basicRescheduler) poll(time int, cluster []*Node)(){
	for _, node := range cluster{
		deleted := 0
		for index, task := range node.tasks{
			//Keep track of the maximum priority seen
			var maxPriority float64 = -1
			var maxPriorityNode *Node

			//Examine each potential home for the task
			for _, candidateNode := range cluster{
				//Ensure that there are enough free resources on the candidate node
				//There are always enough free resources on the task's home node
				freeCores, freeMem := node.freeResources()
				freeMem = freeMem + task.mem
				freeCores = freeCores + task.cores

				if candidateNode == node || (freeCores >= task.cores && freeMem >= task.mem) {
					var priority float64
					if candidateNode == node {
						priority = float64(balancedResourcePriorityExcluding(task, candidateNode, task) + leastRequestedPriorityExcluding(task, candidateNode, task)) + backgroundReschedulingThreshold
					}else{
						priority = float64(balancedResourcePriority(task, candidateNode) + leastRequestedPriority(task, candidateNode))
					}

					if priority > maxPriority{
						maxPriority = priority
						maxPriorityNode = candidateNode
					}
				}
			}

			if maxPriorityNode != node{
				//Delete the old task fom the node's task list
				_ = node.removeTask(index - deleted)
				//Add the new task to the node's task list
				maxPriorityNode.tasks = append(maxPriorityNode.tasks, task)
				deleted++

				fmt.Printf("BackgroundReschedule,%d,%f,%f,%s,%s,%d\n", task.jobID, task.cores, task.mem, maxPriorityNode.name, node.name, time)
			}
		}
	}
	return
}

//rescheduleFailedTask searches for and attempts to execute an appropriate task to preempt
func (basicRescheduler) rescheduleFailedTask (taskToAccomodate *Task, cluster []*Node, time int) (err error){
	scheduler := K8sScheduler{}
	maxPriority := -1
	var maxPriorityNode *Node = nil
	var maxPriorityTaskIndex int = -1

	for _, node := range cluster{
		for i, task := range node.tasks{
			freeCores, freeMem := node.freeResources()
			freeMem = freeMem + task.mem
			freeCores = freeCores + task.cores

			if freeCores >= taskToAccomodate.cores && freeMem >= taskToAccomodate.mem {
				//Check to make sure the task can be rescheduled
				if potentialNode, _ := scheduler.schedule(task, cluster); potentialNode != nil{
					priority := balancedResourcePriorityExcluding(task, node, taskToAccomodate) + leastRequestedPriorityExcluding(task, node, taskToAccomodate)
					if priority > maxPriority{
						maxPriorityNode, maxPriorityTaskIndex = node, i
					}
				}
			}
		}
	}

	if maxPriorityNode == nil{
		return errors.New("No potential tasks for rescheduling")
	}else{

		//Delete the old task fom the node's task list
		bootedTask := maxPriorityNode.removeTask(maxPriorityTaskIndex)
		//Add the new task to the node's task list
		maxPriorityNode.tasks = append(maxPriorityNode.tasks, taskToAccomodate)

		nodeToRescheduleOnto, err := scheduler.schedule(bootedTask, cluster)

		if err != nil{
			return errors.New("Error: Valid new homes for booted tasks have disapeared")
		}
		nodeToRescheduleOnto.tasks = append(nodeToRescheduleOnto.tasks, bootedTask)

		fmt.Printf("Reschedule,%d,%f,%f,%s,%d,%f,%f,%s,%d\n", taskToAccomodate.jobID, taskToAccomodate.cores, taskToAccomodate.mem, maxPriorityNode.name, bootedTask.jobID, bootedTask.cores, bootedTask.mem, nodeToRescheduleOnto.name, time)

		return nil
	}
}

//balancedResourcePriorityExcluding is a modified balancedResourcePriority that does not include the task seeking rescheduling
func balancedResourcePriorityExcluding(task *Task, node *Node, taskExcluding *Task) int{
	totalCores, totalMem := node.usedResources()
	totalCores, totalMem = totalCores + task.cores - taskExcluding.cores, totalMem + task.mem - taskExcluding.mem


	capacityCores, capacityMem := node.cores, node.mem
	cpuFraction := totalCores / capacityCores
	memoryFraction := totalMem / capacityMem
	diff := math.Abs(cpuFraction - memoryFraction)
	return int(10 - diff*10)
}

//leastRequestedPriorityExcluding is a modified leastRequestedPriority that does not include the task seeking rescheduling
func leastRequestedPriorityExcluding(task *Task, node *Node, taskExcluding *Task) int{
	totalCores, totalMem := node.usedResources()
	totalCores, totalMem = totalCores + task.cores - taskExcluding.cores, totalMem + task.mem - taskExcluding.mem

	capacityCores, capacityMem := node.cores, node.mem

	cpuScore := ((capacityCores - totalCores) * 10) / capacityCores
	memScore := ((capacityMem - totalMem) * 10) / capacityMem

	return int((cpuScore + memScore) / 2)
}

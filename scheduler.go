package main

import (
	"math"
	"errors"
)

type K8sScheduler struct {

}

//schedule implements a rough emulation of Kubernetes' scheduling policy
func (K8sScheduler) schedule (task *Task, cluster []*Node) (node *Node, priority int, err error){
	maxPriority := -1
	var maxPriorityNode *Node = nil

	for _, node := range cluster {

		freeCores, freeMem := node.freeResources()
		freeCores, freeMem = freeCores - task.cores, freeMem - task.mem
		if freeCores > 0 && freeMem > 0 {
			priority := balancedResourcePriority(task, node) + leastRequestedPriority(task, node)
			if priority > maxPriority {
				maxPriority = priority
				maxPriorityNode = node
			}
		}
	}

	//This node will equal nil iff no nodes have available CPU and memory
	if maxPriorityNode == nil{
		return nil, 0, errors.New("No free nodes for scheduling")
	}else{
		return maxPriorityNode, maxPriority, nil
	}
}

//balancedResourcePriority assigns a priority to a potential binding according to the relative use of CPU and RAM
func balancedResourcePriority(task *Task, node *Node) int{
	totalCores, totalMem := node.usedResources()
	totalCores, totalMem = totalCores + task.cores, totalMem + task.mem

	capacityCores, capacityMem := node.cores, node.mem
	cpuFraction := totalCores / capacityCores
	memoryFraction := totalMem / capacityMem
	diff := math.Abs(cpuFraction - memoryFraction)
	return int(10 - diff*10)
}

//leastRequestedPriority assigns a priority to a potential binding which favors more empty nodes
func leastRequestedPriority(task *Task, node *Node) int{
	totalCores, totalMem := node.usedResources()
	totalCores, totalMem = totalCores + task.cores, totalMem + task.mem

	capacityCores, capacityMem := node.cores, node.mem

	cpuScore := ((capacityCores - totalCores) * 10) / capacityCores
	memScore := ((capacityMem - totalMem) * 10) / capacityMem

	return int((cpuScore + memScore) / 2)
}
package main

import (
	"math"
	"fmt"
)

type basicDownscaler struct {
}

func (basicDownscaler) rescheduleFailedTask(task *Task, cluster []*Node, time int) (err error){
	return nil
}

func (basicDownscaler) poll(time int, cluster []*Node) ([]*Node){
	threshold := .5
	cpuUtilization, memUtilization := getTotalUtilization(cluster)

	if cpuUtilization < threshold && memUtilization < threshold{

		//We seek the node which gives the highest net priority shift
		//Note that this net value is likely negative
		var maxNode *Node = nil
		var maxNewCluster []*Node= nil
		maxNodePriority := math.MinInt64

		for candidateNodeIndex, candidateNode := range cluster{
			candidatePriority, newClusterState := calculateDownscalePriority(candidateNode, candidateNodeIndex, cluster)

			//If all tasks could be rescheduled
			if newClusterState != nil{
				if candidatePriority > maxNodePriority{
					maxNode, maxNewCluster, maxNodePriority = candidateNode, newClusterState, candidatePriority
				}
			}
		}

		fmt.Printf("Down,%d,%s,%d,%d,%d\n", len(maxNewCluster), maxNode.name, len(maxNode.tasks), maxNodePriority, time)
		return maxNewCluster
	}
	return cluster
}

//Calculate the total net priority change from rescheduling all pods on a node
//It returns the net priority difference from rescheduling all pods off this node, and the simulated state of the cluster after rescheduling
func calculateDownscalePriority(candidateNode *Node, candidateNodeIndex int, cluster []*Node)(int, []*Node){
	//Create a deep copy of the cluster minus the candidate node
	var clusterExcluding []*Node
	for _, srcNode := range cluster{
		if srcNode != candidateNode{
			clusterExcluding = append(clusterExcluding, srcNode.clone())
		}
	}

	netPriority := 0

	for _, taskToReschedule := range candidateNode.tasks{
		newNode, newNodePriority, err := K8sScheduler{}.schedule(taskToReschedule, clusterExcluding)

		//If we cannot rescheduler at least one pod, this node is not a candidate
		if err != nil{
			return math.MinInt64, nil
		}else{
			//Note that we do not update candidateNode.tasks
			//So that currentPriority can be accurately computed
			newNode.tasks = append(newNode.tasks, taskToReschedule)
			currentPriority := balancedResourcePriorityExcluding(taskToReschedule, candidateNode, taskToReschedule) + leastRequestedPriorityExcluding(taskToReschedule, candidateNode, taskToReschedule)
			netPriority = netPriority + newNodePriority - currentPriority
		}
	}

	return netPriority, clusterExcluding
}
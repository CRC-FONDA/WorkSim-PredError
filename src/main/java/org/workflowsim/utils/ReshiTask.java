package org.workflowsim.utils;

import org.workflowsim.scheduling.ReshiSchedulingAlgorithm;

/**
 * models a (task,machine,rank) tuple ranked by the regression tree.
 */
public class ReshiTask implements Comparable {

    String workflow;
    String taskName;
    String node;
    Double rank;

    public ReshiTask(String taskName, String workflow, String node, Double rank) {
        this.workflow = workflow;
        this.taskName = taskName;
        this.node = node;
        this.rank = rank;
    }

    public String get_task_name(){return this.taskName;}

    public String get_machine_name(){return this.node;}

    public double get_rank(){return this.rank;}


    // TODO: shouldn't we check if the two ReshiTasks target the same task?
    @Override
    public int compareTo(Object o) {
        if (this.rank > ((ReshiTask) o).rank) {
            return 1;
        } else if (this.rank < ((ReshiTask) o).rank) {
            return -1;
        } else {
            return 0;
        }
    }
}

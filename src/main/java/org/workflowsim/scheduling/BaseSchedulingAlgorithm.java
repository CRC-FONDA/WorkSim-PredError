/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.Task;

/**
 * The base scheduler has implemented the basic features. Every other scheduling method
 * should extend from BaseSchedulingAlgorithm but should not directly use it. 
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public abstract class BaseSchedulingAlgorithm implements SchedulingAlgorithmInterface {

    /**
     * the job list.
     */
    private List<? extends Cloudlet> cloudletList;
    /**
     * the vm list.
     */
    private List<? extends Vm> vmList;
    /**
     * the scheduled job list.
     */
    private List< Cloudlet> scheduledList;

    /**
     * stores the critical path of each task.
     */
    private Map<Task, Double> task_ranking;

    /**
     * Initialize a BaseSchedulingAlgorithm
     */
    public BaseSchedulingAlgorithm() {
        this.scheduledList = new ArrayList();
    }

    /**
     * Sets the job list.
     *
     * @param list
     */
    @Override
    public void setCloudletList(List list) {
        this.cloudletList = list;
    }

    /**
     * Sets the vm list
     *
     * @param list
     */
    @Override
    public void setVmList(List list) {
        this.vmList = new ArrayList(list);
    }

    /**
     * returns the map containing the critical computation path of each task
     * @return the map.
     */
    public Map<Task, Double> get_task_ranking(){
        return this.task_ranking;
    }

    /**
     * sets the task ranking field.
     * @param ranking the ranking to set it to.
     */
    public void set_task_ranking(Map<Task, Double> ranking){
        this.task_ranking = ranking;
    }

    /**
     * Gets the job list.
     *
     * @return the job list
     */
    @Override
    public List getCloudletList() {
        return this.cloudletList;
    }

    /**
     * Gets the vm list
     *
     * @return the vm list
     */
    @Override
    public List getVmList() {
        return this.vmList;
    }

    /**
     * The main function
     * @throws java.lang.Exception
     */
    @Override
    public abstract void run() throws Exception;

    /**
     * Gets the scheduled job list
     *
     * @return job list
     */
    @Override
    public List getScheduledList() {
        return this.scheduledList;
    }
}

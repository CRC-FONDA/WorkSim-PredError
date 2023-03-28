/**
 * Copyright 2012-2013 University Of Southern California
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.scheduling;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.cloudbus.cloudsim.Cloudlet;
import org.workflowsim.*;

/**
 * MinMin algorithm.
 *
 * @author Weiwei Chen
 * @date Apr 9, 2013
 * @since WorkflowSim Toolkit 1.0
 */
public class MinMinSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    List<LinkedHashMap<String, Object>> arr;

    NormalDistribution normalDistribution;
    Random random;

    public MinMinSchedulingAlgorithm() {
        super();


        arr = MetaGetter.getArr();
        random = new Random();

    }

    private final List<Boolean> hasChecked = new ArrayList<>();

    @Override
    public void run() {

        minmin();

        /*
        int size = getCloudletList().size();
        hasChecked.clear();
        for (int t = 0; t < size; t++) {
            hasChecked.add(false);
        }
        for (int i = 0; i < size; i++) {
            int minIndex = 0;
            Cloudlet minCloudlet = null;
            for (int j = 0; j < size; j++) {
                Job cloudlet = (Job) getCloudletList().get(j);
                if (!hasChecked.get(j)) {
                    minCloudlet = cloudlet;
                    minIndex = j;
                    break;
                }
            }
            if (minCloudlet == null) {
                break;
            }


            for (int j = 0; j < size; j++) {
                Cloudlet cloudlet = (Cloudlet) getCloudletList().get(j);
                if (hasChecked.get(j)) {
                    continue;
                }
                long length = cloudlet.getCloudletLength();
                if (length < minCloudlet.getCloudletLength()) {
                    minCloudlet = cloudlet;
                    minIndex = j;
                }
            }
            hasChecked.set(minIndex, true);

            int vmSize = getVmList().size();
            CondorVM firstIdleVm = null;//(CondorVM)getVmList().get(0);
            for (int j = 0; j < vmSize; j++) {
                CondorVM vm = (CondorVM) getVmList().get(j);
                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                    firstIdleVm = vm;
                    break;
                }
            }
            if (firstIdleVm == null) {
                break;
            }
            for (int j = 0; j < vmSize; j++) {
                CondorVM vm = (CondorVM) getVmList().get(j);
                if ((vm.getState() == WorkflowSimTags.VM_STATUS_IDLE)
                        && vm.getCurrentRequestedTotalMips() > firstIdleVm.getCurrentRequestedTotalMips()) {
                    firstIdleVm = vm;
                }
            }
            firstIdleVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            minCloudlet.setVmId(firstIdleVm.getId());
            getScheduledList().add(minCloudlet);
        }

         */

    }


    public void runReshiBaseline() {

        ArrayList<Integer> checked = new ArrayList<>();

        List<Job> cloudlets = new ArrayList<>(getCloudletList());

        List<CondorVM> vmList = getVmList();

        int size = cloudlets.size();


        if (getCloudletList().size() == 0) {
            return;
        }

        for (int iii = 0; iii < size; iii++) {

            Task minTask = null;

            CondorVM minVm = null;

            long minTime = Long.MAX_VALUE;

            for (Job jobWrapper : cloudlets) {

                if (checked.contains(jobWrapper.getCloudletId())) {
                    continue;
                }

                List<CondorVM> freeVMs = new ArrayList<>();
                for (int l = 0; l < vmList.size(); l++) {
                    CondorVM vm = vmList.get(l);
                    if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                        freeVMs.add(vm);
                    }
                }

                if (freeVMs.size() == 0) {
                    return;
                }

                if (jobWrapper.getTaskList().size() == 0) {
                    minVm = vmList.get((int) Math.round(random.nextDouble() * (vmList.size() - 1)));
                    minTask = jobWrapper;
                    break;
                }

                Task task = jobWrapper.getTaskList().get(0);

                try {

                    for (CondorVM vm : vmList) {

                        long lengthWithNoise = (long) (MetaGetter.getTaskRuntime(task, vm) * MetaGetter.getRandomFactor());

                        if (minTime > lengthWithNoise) {
                            minTask = task;
                            minVm = vm;
                            minTime = lengthWithNoise;
                            System.out.println("Set task " + task.getType() + "(" + minTask.getCloudletId() + ") with runtime " + minTime + " to min");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }

            checked.add(minTask.getCloudletId());
            Task finalMinTask = minTask;
            Job minJob = cloudlets.stream().filter(c -> c.getCloudletId() == finalMinTask.getCloudletId()).collect(Collectors.toList()).get(0);
            minJob.setVmId(minVm.getId());
            minVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            getScheduledList().add(minJob);
        }
        System.out.println(getScheduledList());
    }


    public void minmin() {

        ArrayList<Integer> checked = new ArrayList<>();

        List<Job> cloudlets = new ArrayList<>(getCloudletList());

        List<CondorVM> vmList = getVmList();

        int size = cloudlets.size();

        if (getCloudletList().size() == 0) {
            return;
        }

        for (int i = 0; i < size; i++) {

            Task minTask = null;

            CondorVM minVm = null;

            long minTime = Long.MAX_VALUE;

            if (cloudlets.get(i).getTaskList().size() == 0) {
                minVm = vmList.get((int) Math.round(random.nextDouble() * (vmList.size() - 1)));
                minTask = cloudlets.get(i);
                checked.add(minTask.getCloudletId());
                Task finalMinTask = minTask;
                Job minJob = cloudlets.stream().filter(c -> c.getCloudletId() == finalMinTask.getCloudletId()).collect(Collectors.toList()).get(0);
                minJob.setVmId(minVm.getId());
                minVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                getScheduledList().add(minJob);
                return;
            }

            for (Job job : cloudlets.stream().filter(c -> !checked.contains(c.getTaskList().get(0).getCloudletId())).collect(Collectors.toList())) {
                Task task = job.getTaskList().get(0);

                List<CondorVM> freeVMs = new ArrayList<>();
                for (int l = 0; l < vmList.size(); l++) {
                    CondorVM vm = vmList.get(l);
                    if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                        freeVMs.add(vm);
                    }
                }

                if (freeVMs.size() == 0) {
                    return;
                }

                for (CondorVM vm : freeVMs) {

                    long lengthWithNoise = (long) (MetaGetter.getTaskRuntime(task, vm) * MetaGetter.getRandomFactor());

                    if (minTime > lengthWithNoise) {
                        minTask = task;
                        minVm = vm;
                        minTime = lengthWithNoise;
                    }

                }
            }
            checked.add(minTask.getCloudletId());
            Task finalMinTask = minTask;
            Job minJob = cloudlets.stream().filter(c -> c.getTaskList().get(0).getCloudletId() == finalMinTask.getCloudletId()).collect(Collectors.toList()).get(0);
            minJob.setVmId(minVm.getId());
            minVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            getScheduledList().add(minJob);
        }
    }
}

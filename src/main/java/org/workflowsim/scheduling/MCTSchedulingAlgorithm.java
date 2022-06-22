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

import com.jayway.jsonpath.JsonPath;
import org.workflowsim.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * MCT algorithm
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class MCTSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    List<LinkedHashMap<String, Object>> arr;

    Random random;

    public MCTSchedulingAlgorithm() {
        super();

        arr = MetaGetter.getArr();
        random = new Random();

    }

    @Override
    public void run() {

        /*
        int size = getCloudletList().size();

        for (int i = 0; i < size; i++) {
            Cloudlet cloudlet = (Cloudlet) getCloudletList().get(i);
            int vmSize = getVmList().size();
            CondorVM firstIdleVm = null;

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
                        && (vm.getCurrentRequestedTotalMips() > firstIdleVm.getCurrentRequestedTotalMips())) {
                    firstIdleVm = vm;
                }
            }


            firstIdleVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            cloudlet.setVmId(firstIdleVm.getId());
            getScheduledList().add(cloudlet);
            Log.printLine("Schedules " + cloudlet.getCloudletId() + " with "
                    + cloudlet.getCloudletLength() + " to VM " + firstIdleVm.getId()
                    + " with " + firstIdleVm.getCurrentRequestedTotalMips());
        }
         */

        mct();
    }

    public void mct() {

        if (getCloudletList().size() == 0) {
            return;
        }

        for (int i = 0; i < getCloudletList().size(); i++) {

            List<CondorVM> vmList = getVmList();

            Job job = (Job) getCloudletList().get(i);

            // irgendwo Error hier
            if (job.getTaskList().size() == 0) {
                CondorVM minVm = (CondorVM) getVmList().get((int) Math.round(random.nextDouble() * (getVmList().size() - 1)));
                Task minTask = ((List<Job>) getCloudletList()).get(i);
                Task finalMinTask = minTask;
                Job minJob = ((List<Job>) getCloudletList()).stream().filter(c -> c.getCloudletId() == finalMinTask.getCloudletId()).collect(Collectors.toList()).get(0);
                minJob.setVmId(minVm.getId());
                minVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                getScheduledList().add(minJob);
                return;
            }

            Task task = job.getTaskList().get(0);

            Task minTask = null;

            CondorVM minVm = null;

            long minTime = Long.MAX_VALUE;

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

            for (int j = 0; j < freeVMs.size(); j++) {
                CondorVM vm = freeVMs.get(j);

                AtomicInteger runtimeSum = new AtomicInteger();
                AtomicInteger count = new AtomicInteger();
                this.arr.stream().filter(e -> ((String) e.get("wfName")).contains(MetaGetter.getWorkflow())).forEach(entry -> {

                    if (task.getType().contains(((String) entry.get("taskName"))) &&
                            vm.getName().equals((String) entry.get("instanceType")) &&
                            ((String) entry.get("wfName")).contains(task.getWorkflow())) {
                        runtimeSum.addAndGet((Integer) entry.get("realtime"));
                        count.getAndIncrement();
                    }
                });


                long lengthWithNoise;

                if (count.get() != 0) {
                    lengthWithNoise = (long) ((runtimeSum.get() / count.get()) * MetaGetter.getRandomFactor());
                } else {
                    lengthWithNoise = (long) (task.getCloudletLength() * MetaGetter.getRandomFactor());
                }

                if (minTime > lengthWithNoise) {
                    minTask = task;
                    minVm = vm;
                    minTime = lengthWithNoise;
                }

            }

            Task finalMinTask = minTask;
            Job minJob = ((List<Job>) getCloudletList()).stream().filter(c -> c.getTaskList().get(0).getCloudletId() == finalMinTask.getCloudletId()).collect(Collectors.toList()).get(0);
            minJob.setVmId(minVm.getId());
            minVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            getScheduledList().add(minJob);
        }

    }
}

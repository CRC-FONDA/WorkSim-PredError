package org.workflowsim.scheduling;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.workflowsim.*;
import org.workflowsim.utils.ReshiTask;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class DTR_TD_SchedulingAlgorithm extends BaseSchedulingAlgorithm {


    List<ReshiTask> reshiTaskList;

    private ReshiStrategy reshiStrategy;

    NormalDistribution normalDistribution;
    Random random;


    public DTR_TD_SchedulingAlgorithm(ReshiStrategy reshiStrategy) {
        this.reshiStrategy = reshiStrategy;
        reshiTaskList = new ArrayList<>();

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/config/ranking/DTR_TD_Rank_Export.csv"))) {
            String s = null;
            bufferedReader.readLine();
            while ((s = bufferedReader.readLine()) != null) {
                String[] entries = s.split(",");
                reshiTaskList.add(new ReshiTask(entries[1], entries[0], entries[2], Double.parseDouble(entries[3])));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() throws Exception {

        List<Job> cloudlets = getCloudletList();

        List<CondorVM> vmList = getVmList();

        // V1
        if (reshiStrategy == ReshiStrategy.V1) {
            Collections.sort(cloudlets, (j1, j2) -> {

                int descandantsJ1 = j1.getChildList().size();
                int descandantsJ2 = j2.getChildList().size();

                if (descandantsJ1 < descandantsJ2) {
                    return 1;
                } else if (descandantsJ1 > descandantsJ2) {
                    return -1;
                } else {
                    return 0;
                }

            });
        } else if (reshiStrategy == ReshiStrategy.V2) {
            // V2
            Collections.sort(cloudlets, (j1, j2) -> {

                int descandantsJ1 = totalDescendants(j1);
                int descandantsJ2 = totalDescendants(j2);

                if (descandantsJ1 < descandantsJ2) {
                    return 1;
                } else if (descandantsJ1 > descandantsJ2) {
                    return -1;
                } else {
                    return 0;
                }
            });
        } else if (reshiStrategy == ReshiStrategy.V3) {
            //V3
            Collections.sort(cloudlets, (j1, j2) -> {

                int descandantsJ1 = depthDescendants(j1,0);
                int descandantsJ2 = depthDescendants(j2,0);

                if (descandantsJ1 < descandantsJ2) {
                    return 1;
                } else if (descandantsJ1 > descandantsJ2) {
                    return -1;
                } else {
                    return 0;
                }
            });
        } else if (reshiStrategy == ReshiStrategy.MAX) {
            Collections.sort(cloudlets, (j1, j2) -> {

                long length1 = j1.getCloudletLength();
                long length2 = j2.getCloudletLength();

                if (length1 < length2) {
                    return 1;
                } else if (length1 > length2) {
                    return -1;
                } else {
                    return 0;
                }
            });
        } else if (reshiStrategy == ReshiStrategy.CRITICALPATHREVERSE) {
            Collections.sort(cloudlets, (j1, j2) -> {
                double l1 = get_task_ranking().get(j1.getTaskList().get(0));
                double l2 = get_task_ranking().get(j2.getTaskList().get(0));

                return Double.compare(l1, l2);
            });
        } else if (reshiStrategy == ReshiStrategy.CRITICALPATH) {
            Collections.sort(cloudlets, (j1, j2) -> {
                double l1 = get_task_ranking().get(j1.getTaskList().get(0));
                double l2 = get_task_ranking().get(j2.getTaskList().get(0));

                return Double.compare(l2, l1);
            });

        }

        for (Job task : cloudlets) {


            List<CondorVM> freeVMs = new ArrayList<>();
            for (int l = 0; l < vmList.size(); l++) {
                CondorVM vm = vmList.get(l);
                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                    freeVMs.add(vm);
                }
            }

            if (freeVMs.size() == 0) {
                break;
            }

            String nodeToSchedule = determineBestMachine(task, freeVMs).get_machine_name();

            CondorVM vmToAssign = freeVMs.stream().filter(vm -> vm.getName().equals(nodeToSchedule)).findFirst().get();

            if (vmToAssign == null) {
                break;
            }
            // Problem, dass irgendwie zu wenig Maschinen frei sind
            vmToAssign.setState(WorkflowSimTags.VM_STATUS_BUSY);
            task.setVmId(vmToAssign.getId());
            getScheduledList().add(task);

        }
    }



    private ReshiTask determineBestMachine(Job taskToLookup, List<CondorVM> freeVMs) {

        // Für den initialen Task, welcher die Files fetcht wird random eine Node ausgewählt
        if (taskToLookup.getTaskList().size() == 0) {
            List<ReshiTask> filteredList = reshiTaskList.stream().filter(t -> freeVMs.stream().map(vm -> vm.getName()).collect(Collectors.toList()).contains(t.get_machine_name()))
                    .collect(Collectors.toList());
            //Collections.shuffle(filteredList);
            return filteredList.get(0);
        }
        // Ranking nach dem Task filtern und sortieren
        List<ReshiTask> filteredList = reshiTaskList.stream().filter(t -> (taskToLookup.getTaskList().get(0).getType().contains(t.get_task_name())) || (t.get_task_name().contains(taskToLookup.getTaskList().get(0).getType())))
                .filter(t -> freeVMs.stream().map(vm -> vm.getName()).collect(Collectors.toList()).contains(t.get_machine_name()))
                .collect(Collectors.toList());

        // falls der Task nicht gerankt wurde sollte lieber die schnellste node genommen werden
        if (filteredList.size() == 0) {

            System.out.println("No ranking for:" + taskToLookup.getTaskList().get(0).getType());

            List<ReshiTask> list = reshiTaskList.stream().filter(t -> freeVMs.stream().map(vm -> vm.getName()).collect(Collectors.toList()).contains(t.get_machine_name()))
                    .collect(Collectors.toList());
            // Collections.shuffle(list);
            return list.get(0);
        }

        System.out.println("ranking found for:" + taskToLookup.getTaskList().get(0).getType());

        Collections.sort(filteredList);
        // todo: here we need to use different allocation heuristics
        return filteredList.get(0);
    }


    // V2

    /**
     * perform BFS to get the number of descendants.
     * @param job the starting task
     * @return number of total descendants
     */
    private int totalDescendants(Task job) {
        Queue<Task> queue = new LinkedList<>();
        Map<Task,Boolean> tasks = new HashMap<>();
        int count = 0;
        for (Task t : job.getChildList()){
            queue.add(t);
            tasks.put(t,true);
        }
        while (!queue.isEmpty()){
            Task e = queue.poll();
            count++;
            assert (tasks.containsKey(e));
            for (Task child : e.getChildList()){
                if (!tasks.containsKey(child)){
                    tasks.put(child,true);
                    queue.add(child);
                }
            }
        }
        return count;
    }

    /**
     * perform DFS to get the maximum depth of a task
     * @param job the task to determine the depth of
     * @param depth the starting depth (0 for the starting task)
     * @return the depth of the task
     */
    // V3
    private static int depthDescendants(Task job, int depth) {
        int result = depth;
        for (Task t : job.getChildList()) {
            result = Math.max(depthDescendants(t, depth + 1), depth);
        }
        return result;
    }
}

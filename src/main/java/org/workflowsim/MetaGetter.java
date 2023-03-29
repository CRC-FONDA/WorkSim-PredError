package org.workflowsim;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Vm;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MetaGetter {

    private static NormalDistribution normalDistribution;

    private static ExponentialDistribution exponentialDistribution;

    private static Random random;

    private static double error = 0.5;

    private static String workflow = "viralrecon";

    private static String distribution = "normal";

    private static ArrayList<Integer> seedStack = null;

    private static int listPointeroffset = 0;

    private static int listPointer = 0;

    private static int randPointer = 0;

    private static int randPointerOffset = 0;

    private static String realdaxPath = "src/main/resources/config/dax/";

    private static List<LinkedHashMap<String, Object>> arr;
    private static LinkedHashMap<TaskRuntimeLUTKey, Double> taskRuntimeLUT = null;

    private static List<List<Integer>> clusterSeedIndices = null;

    private static int clusterSize = 40;

    private static double getRandomFromNormalDist() {

        RandomGenerator rg = new JDKRandomGenerator();
        rg.setSeed(seedStack.get(listPointer++));
        normalDistribution = new NormalDistribution(rg, 1, 0.5, 1);

        double factor = -1;
        if (random.nextDouble() > 0.5) {
            factor = (1 + normalDistribution.sample() * error);
        } else {
            factor = Math.max(1 - normalDistribution.sample() * error, 0);
        }
        return factor;
    }

    private static double getRandomFromExponentialDist() {

        RandomGenerator rg = new JDKRandomGenerator();
        rg.setSeed(seedStack.get(listPointer++));
        exponentialDistribution = new ExponentialDistribution(rg, 1, 1);

        double factor = -1;
        if (random.nextDouble() > 0.5) {
            factor = (1 + exponentialDistribution.sample() * error);
        } else {
            factor = Math.max(1 - exponentialDistribution.sample() * error, 0);
        }
        return factor;
    }

    public static double getRandomForCluster() {

        if (random == null) {
            getRandomFactor();
        }

        return random.nextDouble();
    }

    public static List<Integer> getClusterNodeIDs(long clusterID) {
        if (clusterSeedIndices == null) {
            clusterSeedIndices = new ArrayList<>();
        }
        if (clusterSeedIndices.size() <= clusterID) {
            SAXBuilder builder = new SAXBuilder();

            Document dom;
            try {
                dom = builder.build(new File("src/main/resources/config/machines/machines.xml"));
            } catch (JDOMException | IOException e) {
                throw new RuntimeException(e);
            }
            Element root = dom.getRootElement();
            List<Element> availableVMs = root.getChildren().get(0).getChildren("host");
            while (clusterSeedIndices.size() <= clusterID) {
                ArrayList<Integer> new_cluster = new ArrayList<>(clusterSize);
                for (int i = 0; i < clusterSize; i++) {
                    int randomNumber = (int) Math.round(MetaGetter.getRandomForCluster() * (availableVMs.size() - 1));
                    new_cluster.add(randomNumber);
                }
                clusterSeedIndices.add(new_cluster);
            }
        }

        return clusterSeedIndices.get((int) clusterID);
    }

    public static void setClusterSize(int newVal) {
        clusterSize = newVal;
    }

    public static int getClusterSize() {
        return clusterSize;
    }

    static class TaskRuntimeLUTKey {
        String taskName;
        String instanceType;
        String wfName;

        TaskRuntimeLUTKey(String tn, String it, String wfn) {
            taskName = tn;
            instanceType = it;
            wfName = wfn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskRuntimeLUTKey that = (TaskRuntimeLUTKey) o;
            return taskName.equals(that.taskName) && instanceType.equals(that.instanceType) && wfName.equals(that.wfName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskName, instanceType, wfName);
        }
    }

    public static double getTaskRuntime(Task task, Vm vm) {
        if (taskRuntimeLUT == null) {
            taskRuntimeLUT = new LinkedHashMap<>();
        }
        TaskRuntimeLUTKey key = new TaskRuntimeLUTKey(task.getType(), vm.getName(), task.getWorkflow());
        if (taskRuntimeLUT.containsKey(key)) {
            return taskRuntimeLUT.get(key);
        } else {
            double task_runtime = 0;
            AtomicInteger runtimeSum = new AtomicInteger();
            AtomicInteger count = new AtomicInteger();
            // todo: diese forEach frist enorm Rechenzeit -> verbessern
            getArr().stream().filter(e -> ((String) e.get("wfName")).contains(MetaGetter.getWorkflow())).forEach(entry -> {

                if (task.getType().contains(((String) entry.get("taskName"))) &&
                        vm.getName().equals((String) entry.get("instanceType")) &&
                        ((String) entry.get("wfName")).contains(task.getWorkflow())) {
                    runtimeSum.addAndGet((Integer) entry.get("realtime"));
                    count.getAndIncrement();
                }
            });
            if (count.get() != 0) {
                task_runtime = ((double) runtimeSum.get()) / count.get();
                //task_runtime = task.getCloudletLength() / vm.getMips();

            } else {
                task_runtime = task.getCloudletLength() / vm.getMips();
            }

            taskRuntimeLUT.put(key, task_runtime);
            return task_runtime;
        }
    }

    public static double getRandomFactor() {

        BufferedReader bufferedReader;

        if (seedStack == null) {
            try {
                seedStack = new ArrayList<>();
                bufferedReader = new BufferedReader(new FileReader("src/main/resources/config/seed.txt"));
                Arrays.stream(bufferedReader.readLine().split(",")).forEach(item -> {
                    seedStack.add(Integer.parseInt(item));
                });
                bufferedReader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        random = new Random(seedStack.get(randPointer));


        if (distribution.equals("exponential")) {
            return getRandomFromExponentialDist();
        } else if (distribution.equals("normal")) {
            return getRandomFromNormalDist();
        } else {
            return -1;
        }

    }

    public static String getRealdaxPath() {
        return realdaxPath;
    }

    public static List<LinkedHashMap<String, Object>> getArr() {
        if (arr == null) {

            try {
                java.io.File f = new java.io.File("src/main/resources/config/runtimes/runtimes_pp.json");
                arr = JsonPath.read(f, "$");
                arr = arr.stream().filter(e -> ((String) e.get("wfName")).contains(MetaGetter.getWorkflow())).collect(Collectors.toList());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return arr;
    }


    public static void resetGenerator() {
        listPointer = listPointeroffset + 0;
        randPointer = randPointerOffset + 0;
        random = new Random(seedStack.get(randPointer));

    }

    public static String getWorkflow() {
        return workflow;
    }

    public static double getError() {
        return error;
    }

    public static String getDistribution() {
        return distribution;
    }

    public static void setError(double error) {
        MetaGetter.error = error;
    }

    public static void setWorkflow(String workflow) {
        MetaGetter.workflow = workflow;
    }

    public static void setDistribution(String distribution) {
        MetaGetter.distribution = distribution;
    }

    public static void setListPointeroffset(int listPointeroffset) {
        MetaGetter.listPointeroffset = listPointeroffset;
    }

    public static void setRandPointerOffset(int randPointerOffset) {
        MetaGetter.randPointerOffset = randPointerOffset;
    }
}

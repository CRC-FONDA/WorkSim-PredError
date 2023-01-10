package org.workflowsim;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
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

    public static String getRealdaxPath(){
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

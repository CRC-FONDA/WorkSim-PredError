package org.workflowsim.utils;
import org.apache.commons.math3.util.Pair;

import java.util.HashMap;
import java.util.Map;

public class BenchmarkSet {
    String job_name;
    String machine_name;
    private Map<String,Double> benchmarks;

    public BenchmarkSet(String job, String machine, HashMap<String, Double> benchmarks){
        job_name = job;
        machine_name = machine;
        this.benchmarks = benchmarks;
    }

    public static Pair<BenchmarkSet, BenchmarkSet> distill_benchmarks(ReshiTask task){
        //todo: distill the benchmark values of both the task and the machine and
        // return them as pair.
        return null;
    }

    public Map<String, Double> get_benchmarks(){
        return this.benchmarks;
    }

}

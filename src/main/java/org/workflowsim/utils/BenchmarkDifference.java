package org.workflowsim.utils;

public class BenchmarkDifference implements ReshiTaskCostFunction{

    /**
     * calculates the sum of differences.
     * Assumes the other benchmark set s2 is greater or equal to s1.
     * @param s1 the first benchmark set
     * @param s2 the second benchmark set
     * @return the sum of differences (s2-s1)
     */
    @Override
    public double compare(BenchmarkSet s1, BenchmarkSet s2) {
        assert (s1.get_benchmarks().keySet().equals(s2.get_benchmarks().keySet()));
        double res = 0.0;
        for (String key : s1.get_benchmarks().keySet()){
            if (s2.get_benchmarks().get(key) < s1.get_benchmarks().get(key)){
                throw new RuntimeException("the other benchmark set is not strictly greater than this one.");
            }
            res += s2.get_benchmarks().get(key)-s1.get_benchmarks().get(key);
        }
        return res;
    }
}

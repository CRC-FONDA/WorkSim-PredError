package examples.org.workflowsim.examples;

import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;

import java.util.Random;

public class TestNodes {

    public static void main(String[] args) {

        // Instantiate a "Mersenne-Twister" generator with a factory method.
        UniformRandomProvider rng = RandomSource.MT.create();



        Random r = new Random();
        for(int i = 0; i<10000; i++) {
            System.out.println(rng.nextInt(270)/10);
        }

    }
}

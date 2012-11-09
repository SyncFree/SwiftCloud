package sys.stats.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import swift.utils.Pair;
import sys.stats.CountingSourceImpl;

public class CountingSourceTester {

    @Test
    public void testNumOps() {

        CountingSourceImpl counter = new CountingSourceImpl("test", 200);
        for (int i = 0; i < 10; i++) {
            counter.incCounter();
        }

        assertEquals(10, counter.getTotalOperations());
    }

    @Test
    //Test if the Counter stores operations according to the time they were executed 
    public void testOpsMoreThan1Second() {

        CountingSourceImpl counter = new CountingSourceImpl("test", 200);
        for (int i = 0; i < 10; i++) {
            counter.incCounter();
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 200; i++) {
            counter.incCounter();
        }

        List<Pair<Long, Double>> values = counter.getCountOverTime();
        int sum = 0;

        for (Pair<Long, Double> value : values) {
            if (value.getFirst() >= 1000l)
                sum += value.getSecond().intValue();
        }
        assertEquals(200, sum);
    }

}

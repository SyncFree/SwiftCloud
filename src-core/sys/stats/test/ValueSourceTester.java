package sys.stats.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import swift.utils.Pair;
import sys.stats.ValueSourceImpl;
import sys.stats.ValuesSource;
import sys.stats.ValuesSource.Stopper;

public class ValueSourceTester {

    @Test
    public void testOpLatency1() {

        ValuesSource opsLatency = new ValueSourceImpl("test", 200);
        Stopper stopper = opsLatency.createEventRecordingStopper();
        stopper.stop();
        // Latency should be under 200ms.
        for (Pair<Long, Double> value : opsLatency.getLatencyHistogram()) {
            long opDuration = value.getFirst();
            assertEquals(200, opDuration);
        }

    }

    @Test
    public void testOpLatency2() {
        ValueSourceImpl opsLatency = new ValueSourceImpl("test", 200);
        Stopper stopper = opsLatency.createEventRecordingStopper();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();
        // Latency should be under 600ms.
        for (Pair<Long, Double> value : opsLatency.getLatencyHistogram()) {
            long opDuration = value.getFirst();
            if (value.getSecond().intValue() > 0)
                assertEquals(600, opDuration);
            else
                assertEquals(0, value.getSecond().intValue());
        }

    }

    @Test
    public void testOpLatency3() {
        ValueSourceImpl opsLatency = new ValueSourceImpl("test", 200);
        Stopper stopper = opsLatency.createEventRecordingStopper();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();

        stopper = opsLatency.createEventRecordingStopper();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();

        // Latency should be under 600ms for successive operations.
        for (Pair<Long, Double> value : opsLatency.getLatencyHistogram()) {
            long opDuration = value.getFirst();
            if (value.getSecond().intValue() > 0) {
                assertEquals(600, opDuration);
                assertEquals(2, value.getSecond().intValue());
            } else
                assertEquals(0, value.getSecond().intValue());
        }

    }

    @Test
    public void testAverageOverTime() {
        ValueSourceImpl opsLatency = new ValueSourceImpl("test", 200);

        opsLatency.recordEventWithValue(20);
        opsLatency.recordEventWithValue(20);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        opsLatency.recordEventWithValue(60);
        opsLatency.recordEventWithValue(40);
        opsLatency.recordEventWithValue(20);

        for (Pair<Long, Double> value : opsLatency.getAverageOverTime()) {
            if (value.getSecond().intValue() > 0 && value.getFirst() == 200) {
                assertEquals(20, value.getSecond().intValue());
            } else if (value.getSecond().intValue() > 0 && value.getFirst() == 1200) {
                assertEquals(40, value.getSecond().intValue());
            }
        }

    }

}

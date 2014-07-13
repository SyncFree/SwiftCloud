package com.yahoo.ycsb.generator;

/**
 * An integer generator that draws integers from a precached pool of integers
 * acquired from the provided origin generator, according to a provided access
 * distribution inside a pool.
 * 
 * @author mzawirski
 */
public class CachedPoolIntegerGenerator extends IntegerGenerator {
    private final int[] pool;
    private final double mean;
    private final IntegerGenerator poolKeyChooser;

    public CachedPoolIntegerGenerator(IntegerGenerator originGenerator, int poolSize, IntegerGenerator poolKeyChooser) {
        this.poolKeyChooser = poolKeyChooser;
        this.pool = new int[poolSize];
        double accum = 0;
        for (int i = 0; i < poolSize; i++) {
            pool[i] = originGenerator.nextInt();
            accum += pool[i];
        }
        mean = accum / poolSize;
        nextInt();
    }

    @Override
    public int nextInt() {
        final int result = pool[poolKeyChooser.nextInt()];
        setLastInt(result);
        return result;
    }

    @Override
    public double mean() {
        return mean;
    }
}

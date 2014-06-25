package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

/**
 * A decorator to integer generator that statically spreads (scrambles) values
 * of the decorated generator across the range of output values. The scrambling
 * is unique, according to the provided scrambling seed. This class can be used
 * to implement per-client thread access locality.
 * 
 * @author mzawirski
 */
public class ScrambledIntegerGeneratorDecorator extends IntegerGenerator {

    private IntegerGenerator decoratedGenerator;
    private int decoratedRange;
    private int scramblingSeed;

    public ScrambledIntegerGeneratorDecorator(IntegerGenerator decoratedGenerator, int decoratedRange,
            int scramblingSeed) {
        this.decoratedGenerator = decoratedGenerator;
        this.decoratedRange = decoratedRange;
        this.scramblingSeed = scramblingSeed;
    }

    @Override
    public int nextInt() {
        final int decoratedResult = decoratedGenerator.nextInt();
        // TODO: take care of uniformity better? hash * ~2^32 / decoratedRange
        final int result = Utils.FNVhash32(decoratedResult + scramblingSeed) % decoratedRange;
        setLastInt(result);
        return result;
    }

    @Override
    public double mean() {
        // Best effort: it may not be true for all distributions due to
        // scrambling.
        return decoratedGenerator.mean();
    }
}

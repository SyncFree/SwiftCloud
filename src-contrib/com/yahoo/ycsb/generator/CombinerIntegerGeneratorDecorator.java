package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

/**
 * Integer number generator that combines two given generators according to a
 * predefined proportion of requests served by each one.
 * 
 * @author mzawirski
 */
public class CombinerIntegerGeneratorDecorator extends IntegerGenerator {
    private final IntegerGenerator primaryGenerator;
    private final IntegerGenerator secondaryGenerator;
    private final double secondaryProportion;

    public CombinerIntegerGeneratorDecorator(IntegerGenerator primaryGenerator, IntegerGenerator secondaryGenerator,
            double secondaryProportion) {
        this.primaryGenerator = primaryGenerator;
        this.secondaryGenerator = secondaryGenerator;
        this.secondaryProportion = secondaryProportion;
    }

    @Override
    public int nextInt() {
        final int result;
        if (Utils.random().nextDouble() < secondaryProportion) {
            result = secondaryGenerator.nextInt();
        } else {
            result = primaryGenerator.nextInt();
        }
        setLastInt(result);
        return result;
    }

    @Override
    public double mean() {
        return primaryGenerator.mean() * (1 - secondaryProportion) + secondaryGenerator.mean() * secondaryProportion;
    }
}
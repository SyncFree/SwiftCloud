package com.yahoo.ycsb.generator;

/**
 * TODO
 * 
 * @author mzawirski
 */
public class IntegerGeneratorRemappingDecorator extends IntegerGenerator {

    private IntegerGenerator decoratedGenerator;
    private int decoratedRange;

    public IntegerGeneratorRemappingDecorator(IntegerGenerator decoratedGenerator, int decoratedRange,
            long remappingSeed) {
        this.decoratedGenerator = decoratedGenerator;
        this.decoratedRange = decoratedRange;
    }

    @Override
    public int nextInt() {
        final int result = decoratedGenerator.nextInt();
        // TODO compute permutation!
        setLastInt(result);
        return result;
    }

    @Override
    public double mean() {
        // mean-ingless due to remapped output?
        return 0;
    }
}

/**
 * Replication Benchmarker
 * https://github.com/score-team/replication-benchmarker/
 * Copyright (C) 2012 LORIA / Inria / SCORE Team
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package loria.swift.application.filesynchroniser;

import java.util.Random;

/**
 * A random with an helper to generate gaussian random numbers.s 
 * @author urso
 */
public class RandomGauss extends Random {
	public RandomGauss() {
		super();
	}

	public RandomGauss(long seed) {
		super(seed);
	}

	/**
	 * Returns the next strictly positive pseudorandom, Gaussian ("normally")
	 * distributed double value with the specified mean and the specified
	 * standard deviation from this random number generator's sequence.
	 * 
	 * @param mean
	 *            the mean
	 * @param sdv
	 *            the standard deviation
	 * @return the next pseudorandom, Gaussian ("normally") distributed strictly
	 *         positive double value
	 */
    public double nextGaussian(double mean, double sdv) {
        double x;
        do {
            x = (this.nextGaussian() * sdv) + mean;
        } while (x <= 0);
        return x;
    }
    
    /**
     * Returns the next strictly positive pseudorandom, Gaussian ("normally") 
     * distributed double value with the specified mean and the specified 
     * standard deviation from this random number generator's sequence.
     * @param mean the mean 
     * @param sdv the standard deviation 
     * @return the next pseudorandom, Gaussian ("normally") distributed strictly positive double value
     */
    public long nextLongGaussian(double mean, double sdv) {
        return 1+ (long) nextGaussian(mean-1, sdv);
    }
}

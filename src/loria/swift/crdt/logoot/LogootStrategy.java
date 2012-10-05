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
package loria.swift.crdt.logoot;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import swift.clocks.TripleTimestamp;

/**
 * Helper for logoot identifier construction.
 * @author urso
 */
public abstract class LogootStrategy implements Serializable {
    /**
     * Generate N identifier between P and Q;
     */
    abstract ArrayList<LogootIdentifier> generateLineIdentifiers(LogootDocument replica, LogootIdentifier P, LogootIdentifier Q, int N);

    static LogootIdentifier constructIdentifier(List<Long> digits, LogootIdentifier P, LogootIdentifier Q, TripleTimestamp nextTimestamp) {
        LogootIdentifier R = new LogootIdentifier(digits.size());
        int i = 0, index = digits.size() - 1; 
        while (i < index && i < P.length() && digits.get(i) == P.getDigitAt(i)) {
            R.addComponent(P.getComponentAt(i).clone());
            i++;
        }
        while (i < index && i < Q.length() && digits.get(i) >= Q.getDigitAt(i)) {
            R.addComponent(Q.getComponentAt(i).clone());
            i++;
        }
        while (i <= index) {
            R.addComponent(new Component(digits.get(i), nextTimestamp));
            i++;
        }
        return R;
    }

    /**
     * An identifier as a biginteger.
     */
    static BigInteger big(LogootIdentifier id, int index, BigInteger base) {
        BigInteger bi = BigInteger.valueOf(id.getDigitAt(0));
        for (int i = 1; i <= index; i++) {
            bi = bi.multiply(base).add(BigInteger.valueOf(id.getDigitAt(i)));
        }
        return bi;
    }
    
    static List<Long> plus(List<Long> lid, long sep, BigInteger base, long max) {
        int index = lid.size() - 1;
        long last = lid.get(index);
        if (max - last < sep) {
            BigInteger dr[] = BigInteger.valueOf(last).add(BigInteger.valueOf(sep)).divideAndRemainder(base);
            lid.set(index, dr[1].longValue());
            while (dr[0].longValue() != 0) {
                --index;
                dr = BigInteger.valueOf(lid.get(index)).add(dr[0]).divideAndRemainder(base);
                lid.set(index, dr[1].longValue());            
            }
        } else {
            lid.set(index, last + sep); 
        }
        return lid;
    }
}

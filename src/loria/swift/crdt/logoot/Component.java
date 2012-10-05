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
import swift.clocks.TripleTimestamp;

public class Component implements Comparable<Component>, Serializable {

    final private long digit;
    final private TripleTimestamp ts;

    public Component(long d, TripleTimestamp ts) {
        this.digit = d;
        this.ts = ts;
    }

    public long getDigit() {
        return digit;
    }

    public TripleTimestamp getTs() {
        return ts;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + (int) (this.digit ^ (this.digit >>> 32));
        hash = 29 * hash + (this.ts != null ? this.ts.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Component other = (Component) obj;
        if (this.digit != other.digit) {
            return false;
        }
        if (this.ts != other.ts && (this.ts == null || !this.ts.equals(other.ts))) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return "<" + digit + '@' + ts + '>';
    }

    @Override
    public int compareTo(Component t) {
        if (this.digit == t.digit) {
            return this.ts.compareTo(t.ts);
        } else {
            return (this.digit - t.digit > 0) ? 1 : -1;
        }
    }

    @Override
    public Component clone() {
        return new Component(digit, ts);
    }
}

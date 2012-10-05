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

import java.util.ArrayList;

/**
 *
 * @author urso
 */
public class RangeList<T> extends ArrayList<T> {

    /**
     * Removes from this list offset elements starting from i. 
     * Shifts any succeeding elements to the left (reduces their index). 
     * This call shortens the list by offset elements. 
     * (If offset==0, this operation has no effect.)
     * @param toIndex index of first element to be removed
     * @param offset number of element to be removed
     */
    public void removeRangeOffset(int toIndex, int offset) {
        removeRange(toIndex, toIndex + offset);
    }
}

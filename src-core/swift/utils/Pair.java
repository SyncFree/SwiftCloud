/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.utils;

import java.io.Serializable;

public class Pair<F, S> implements Serializable {

    private static final long serialVersionUID = 1L;
    private F first;
    private S second;

    // required for kryo
    public Pair() {
    }

    public Pair(Pair<F, S> op) {
        this.first = op.getFirst();
        this.second = op.getSecond();
    }

    public Pair(F first, S second) {
        super();
        this.first = first;
        this.second = second;
    }

    public F getFirst() {
        return first;
    }

    public void setFirst(F first) {
        this.first = first;
    }

    public S getSecond() {
        return second;
    }

    public void setSecond(S second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Pair) {
            Pair<?, ?> other = (Pair<?, ?>) o;
            if (first == null && other.first != null || other.first == null && first != null) {
                return false;
            }
            if (second == null && other.second != null || other.second == null && second != null) {
                return false;
            }
            return first.equals(other.getFirst()) && second.equals(other.getSecond());
        }
        return false;
    }

    @Override
    public String toString() {
        return "(" + ((first != null) ? first.toString() : "NULL") + ";"
                + ((second != null) ? second.toString() : "NULL") + ")";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

}

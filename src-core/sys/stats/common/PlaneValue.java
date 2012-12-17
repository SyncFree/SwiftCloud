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
package sys.stats.common;

/**
 * Represents a Two-Dimensional value
 * 
 * @author balegas
 **/
public class PlaneValue<X, Y> {

    private X x;
    private Y y;

    public PlaneValue() {

    }

    public PlaneValue(X x, Y y) {
        super();
        this.x = x;
        this.y = y;
    }

    public X getX() {
        return x;
    }

    public void setX(X x) {
        this.x = x;
    }

    public Y getY() {
        return y;
    }

    public void setY(Y y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return "(" + ((x != null) ? x.toString() : "NULL") + ";" + ((y != null) ? y.toString() : "NULL") + ")";
    }

}

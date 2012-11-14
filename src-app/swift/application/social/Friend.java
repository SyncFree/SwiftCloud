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
package swift.application.social;

import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Copyable;

public class Friend implements Copyable, java.io.Serializable {
    String name;
    CRDTIdentifier userId;

    /** DO NOT USE: Empty constructor required by Kryo */
    Friend() {
    }

    public Friend(final String name, final CRDTIdentifier userId) {
        this.name = name;
        this.userId = userId.clone();
    }

    @Override
    public Object copy() {
        return new Friend(this.name, userId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || !(obj instanceof Friend)) {
            return false;
        }
        Friend other = (Friend) obj;
        return this.name.equals(other.name) && this.userId.equals(other.userId);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = result * 37 + userId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return this.name;
    }

}

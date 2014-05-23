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
package swift.crdt.core;

import java.io.Serializable;

/**
 * System-wide unique identifier for CRDT object. Identification via table to
 * which the CRDT is associated and key under which the CRDT is stored.
 * 
 * @author annettebieniusa
 * 
 */
// TODO: Add type (w/generics) as part of id?
public class CRDTIdentifier implements Cloneable, Serializable, Comparable<CRDTIdentifier> {
    private static final long serialVersionUID = 1L;
    private String table;
    private String key;
    private transient String id;

    /** Do not use: This constructor is required for Kryo */
    public CRDTIdentifier() {
    }

    public CRDTIdentifier(String table, String key) {
        if (table == null || "".equals(table) || key == null || "".equals(key)) {
            throw new NullPointerException("CRDTIdentifier cannot have empty table or key");
        }
        this.table = table;
        this.key = key;
    }

    private String getId() {
        if (id == null) {
            id = "(" + table + ";" + key + ")";
        }
        return id;
    }

    /**
     * @return table for an object to which the object is associated
     */
    public String getTable() {
        return this.table;
    }

    /**
     * @return key for an object under which the object is saved
     */
    public String getKey() {
        return this.key;
    }

    @Override
    public int hashCode() {
        return table.hashCode() ^ key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CRDTIdentifier)) {
            return false;
        }
        CRDTIdentifier other = (CRDTIdentifier) obj;
        return table.equals(other.table) && key.equals(other.key);
    }

    @Override
    public String toString() {
        return getId();
    }

    @Override
    public int compareTo(CRDTIdentifier arg0) {
        return getId().compareTo(arg0.getId());
    }

    @Override
    public CRDTIdentifier clone() {
        return new CRDTIdentifier(table, key);
    }
}

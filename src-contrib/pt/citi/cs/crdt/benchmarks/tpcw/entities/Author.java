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
package pt.citi.cs.crdt.benchmarks.tpcw.entities;

import java.util.TreeMap;

import org.uminho.gsd.benchmarks.interfaces.Entity;

public class Author implements Entity {

    public String A_FNAME;
    public String A_LNAME;
    public String A_MNAME;
    public String A_BIO;
    public String A_DOB;
    public int A_ID;

    Author() {
    }

    public Author(int id, String fname, String Lname, String mname, String dob, String bio) {
        this.A_ID = id;
        A_FNAME = fname;
        A_LNAME = Lname;
        A_BIO = bio;
        A_DOB = dob;
        A_MNAME = mname;
    }

    public String getA_FNAME() {
        return A_FNAME;
    }

    public void setA_FNAME(String a_FNAME) {
        A_FNAME = a_FNAME;
    }

    public String getA_LNAME() {
        return A_LNAME;
    }

    public void setA_LNAME(String a_LNAME) {
        A_LNAME = a_LNAME;
    }

    public String getA_MNAME() {
        return A_MNAME;
    }

    public void setA_MNAME(String a_MNAME) {
        A_MNAME = a_MNAME;
    }

    public String getABIO() {
        return A_BIO;
    }

    public void setABIO(String aBIO) {
        A_BIO = aBIO;
    }

    public String getA_DOB() {
        return A_DOB;
    }

    public void setA_DOB(String A_DOB) {
        this.A_DOB = A_DOB;
    }

    public int getAuthor_id() {
        return A_ID;
    }

    public void setAuthor_id(int A_ID) {
        this.A_ID = A_ID;
    }

    public String getKeyName() {
        return "A_ID";
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("A_FNAME", A_FNAME);
        values.put("A_LNAME", A_LNAME);
        values.put("A_MNAME", A_MNAME);
        values.put("A_DOB", A_DOB);
        values.put("A_BIO", A_BIO);

        return values;
    }

    @Override
    public Object copy() {
        return new Author(A_ID, A_FNAME, A_LNAME, A_MNAME, A_DOB, A_BIO);
    }

}
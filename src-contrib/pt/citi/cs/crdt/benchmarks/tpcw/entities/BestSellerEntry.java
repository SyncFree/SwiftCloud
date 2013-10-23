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

import swift.crdt.interfaces.Copyable;

public class BestSellerEntry implements Copyable {

    private String I_SUBJECT;
    private int I_ID;
    private int I_TOTAL_SOLD;

    BestSellerEntry() {
    }

    public BestSellerEntry(String i_SUBJECT, int i_ID, int i_TOTAL_SOLD) {
        super();
        I_SUBJECT = i_SUBJECT;
        I_ID = i_ID;
        I_TOTAL_SOLD = i_TOTAL_SOLD;
    }

    public String getI_SUBJECT() {
        return I_SUBJECT;
    }

    public int getI_ID() {
        return I_ID;
    }

    public int getI_TOTAL_SOLD() {
        return I_TOTAL_SOLD;

    }

    @Override
    public Object copy() {
        return new BestSellerEntry(I_SUBJECT, I_ID, I_TOTAL_SOLD);
    }

    @Override
    public String toString() {
        return "{" + I_ID + " " + I_TOTAL_SOLD + " " + I_SUBJECT + "}";
    }

}

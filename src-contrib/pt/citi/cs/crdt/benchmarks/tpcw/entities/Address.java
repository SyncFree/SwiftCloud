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

public class Address implements Entity {

    public String ADDR_STREET1;
    public String ADDR_STREET2;
    public String ADDR_CITY;
    public String ADDR_STATE;
    public String ADDR_ZIP;
    public int ADDR_CO_ID;
    public String key;

    Address() {
    }

    public Address(String key, String street1, String street2, String city, String state, String zip, int country) {
        this.ADDR_STREET1 = street1;
        this.ADDR_STREET2 = street2;
        this.ADDR_CITY = city;
        this.ADDR_STATE = state;
        this.ADDR_ZIP = zip;
        this.ADDR_CO_ID = country;
        this.key = key;
    }

    public String getAddress_id() {
        return key;
    }

    public void setAddress_id(String ADDR_ID) {
        this.key = ADDR_ID;
    }

    public String getCity() {
        return ADDR_CITY;
    }

    public void setCity(String city) {
        this.ADDR_CITY = city;
    }

    public int getCountry_id() {
        return ADDR_CO_ID;
    }

    public void setCountry_id(int country_id) {
        this.ADDR_CO_ID = country_id;
    }

    public String getState() {
        return ADDR_STATE;
    }

    public void setState(String state) {
        this.ADDR_STATE = state;
    }

    public String getStreet1() {
        return ADDR_STREET1;
    }

    public void setStreet1(String street1) {
        this.ADDR_STREET1 = street1;
    }

    public String getStreet2() {
        return ADDR_STREET2;
    }

    public void setStreet2(String street2) {
        this.ADDR_STREET2 = street2;
    }

    public String getZip() {
        return ADDR_ZIP;
    }

    public void setZip(String zip) {
        this.ADDR_ZIP = zip;
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("ADDR_STREET1", ADDR_STREET1);
        values.put("ADDR_STREET2", ADDR_STREET2);
        values.put("ADDR_CITY", ADDR_CITY);
        values.put("ADDR_STATE", ADDR_STATE);
        values.put("ADDR_ZIP", ADDR_ZIP);
        values.put("ADDR_CO_ID", ADDR_CO_ID);

        return values;
    }

    public String getKeyName() {
        return "key";
    }

    @Override
    public Object copy() {
        return new Address(key, ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE, ADDR_ZIP, ADDR_CO_ID);
    }

}
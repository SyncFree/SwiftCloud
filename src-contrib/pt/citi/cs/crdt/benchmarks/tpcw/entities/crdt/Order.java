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
package pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt;

import java.text.ParseException;
import java.util.Collection;
import java.util.TreeMap;

import org.uminho.gsd.benchmarks.interfaces.Entity;

import pt.citi.cs.crdt.benchmarks.tpcw.entities.TPCWNamingScheme;
import swift.crdt.AddWinsSetCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.Copyable;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class Order implements Copyable, Entity {

    private String O_ID;
    private String O_C_ID;
    private float O_SUB_TOTAL;
    private float O_TAX;
    private float O_TOTAL;
    private String O_SHIP_TYPE;
    private String O_SHIP_ADDR;
    private String O_STATUS;
    private String O_BILL_ADDR_ID;
    private long O_DATE;
    private long O_SHIP_DATE;

    // TODO: In fact it doesn't have to be a crdt
    private CRDTIdentifier orderlines;

    Order() {
    }

    public Order(String O_ID, String O_C_ID, long O_DATE, float O_SUB_TOTAL, float O_TAX, float O_TOTAL,
            String O_SHIP_TYPE, long O_SHIP_DATE, String O_STATUS, String O_BILL_ADDR_ID, String O_SHIP_ADDR) {
        this.O_ID = O_ID;
        this.O_C_ID = O_C_ID;
        this.O_SUB_TOTAL = O_SUB_TOTAL;
        this.O_TAX = O_TAX;
        this.O_TOTAL = O_TOTAL;
        this.O_SHIP_TYPE = O_SHIP_TYPE;
        this.O_SHIP_ADDR = O_SHIP_ADDR;
        this.O_STATUS = O_STATUS;
        this.O_BILL_ADDR_ID = O_BILL_ADDR_ID;
        this.O_DATE = O_DATE;
        this.O_SHIP_DATE = O_SHIP_DATE;

        orderlines = TPCWNamingScheme.forOrderLines(O_ID);
    }

    public void addOrderLine(OrderLine line, TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        AddWinsSetCRDT<OrderLine> orderLines = handler.get(orderlines, true, AddWinsSetCRDT.class);
        orderLines.add(line);
        // TODO: update the order attributes if any
    }

    public OrderLine getOrderLine(String id, TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        AddWinsSetCRDT<OrderLine> orderLines = handler.get(orderlines, true, AddWinsSetCRDT.class);
        for (OrderLine ol : orderLines.getValue()) {
            if (ol.OL_ID == id)
                return ol;
        }
        return null;
    }

    public void setOrderLine(int id, OrderLine ol, TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        AddWinsSetCRDT<OrderLine> orderLines = handler.get(orderlines, true, AddWinsSetCRDT.class);
        orderLines.add(ol);

    }

    public Collection<OrderLine> getOrderLines(TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        AddWinsSetCRDT<OrderLine> orderLines = handler.get(orderlines, true, AddWinsSetCRDT.class);
        return orderLines.getValue();
    }

    @Override
    public Object copy() {

        return new Order(O_ID, O_C_ID, O_DATE, O_SUB_TOTAL, O_TAX, O_TOTAL, O_SHIP_TYPE, O_SHIP_DATE, O_STATUS,
                O_BILL_ADDR_ID, O_SHIP_ADDR);
    }

    public String getO_ID() {
        return O_ID;
    }

    public String getO_C_ID() {
        return O_C_ID;
    }

    public String getO_BILL_ADDR_ID() {
        return O_BILL_ADDR_ID;
    }

    @Override
    public String getKeyName() {
        System.out.println("NOT IMPLEMENTED");
        return null;
    }

    @Override
    public TreeMap<String, Object> getValuesToInsert() {
        System.out.println("NOT IMPLEMENTED");
        return null;
    }

    public long getO_DATE() throws ParseException {
        return O_DATE;
    }

    public OrderInfo getInfo() {
        return new OrderInfo(O_ID, O_DATE);
    }

}

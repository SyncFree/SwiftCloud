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

import java.util.Collection;

import pt.citi.cs.crdt.benchmarks.tpcw.entities.TPCWNamingScheme;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class ShoppingCart implements Copyable {

    private String SC_C_ID;
    private String SC_DATE;
    private String SC_C_FNAME;
    private String SC_C_LNAME;
    private float SC_C_DISCOUNT;
    private CRDTIdentifier cartItems;

    public ShoppingCart() {
    }

    public ShoppingCart(String cart_id) {
        this.SC_C_ID = cart_id;
        this.cartItems = TPCWNamingScheme.forShoppingCartItems(cart_id);
    }

    public ShoppingCart(String sC_C_ID, String sC_DATE, String sC_C_FNAME, String sC_C_LNAME, float sC_C_DISCOUNT) {
        super();
        SC_C_ID = sC_C_ID;
        SC_DATE = sC_DATE;
        SC_C_FNAME = sC_C_FNAME;
        SC_C_LNAME = sC_C_LNAME;
        SC_C_DISCOUNT = sC_C_DISCOUNT;
        this.cartItems = TPCWNamingScheme.forShoppingCartItems(sC_C_ID);
    }

    public void addSCLine(SCLine line, TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        SetTxnLocalShoppingCart cart = handler.get(cartItems, true, SetShoppingCart.class);
        cart.insert(line);
    }

    public SCLine getSCLine(String itemID, TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        SetTxnLocalShoppingCart cart = handler.get(cartItems, true, SetShoppingCart.class);
        for (SCLine scline : cart.getValue()) {
            if ((scline.getI_ID() + "").equals(itemID))
                return scline;
        }
        return null;
    }

    public Collection<SCLine> getSCLines(TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        SetTxnLocalShoppingCart cart = handler.get(cartItems, true, SetShoppingCart.class);
        return cart.getValue();

    }

    public String getSC_C_ID() {
        return SC_C_ID;
    }

    public void setSC_C_ID(String SC_C_ID) {
        this.SC_C_ID = SC_C_ID;
    }

    public String getSC_DATE() {
        return SC_DATE;
    }

    public void setSC_DATE(String SC_DATE) {
        this.SC_DATE = SC_DATE;
    }

    public float getSC_SHIP_COST(TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        float price = 3;
        SetTxnLocalShoppingCart cart = handler.get(cartItems, true, SetShoppingCart.class);
        for (SCLine line : cart.getValue())
            price += line.getSCL_QTY(handler);
        return price;
    }

    public float getSC_SUB_TOTAL(TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        float price = 0;
        SetTxnLocalShoppingCart cart = handler.get(cartItems, true, SetShoppingCart.class);
        for (SCLine line : cart.getValue())
            price += line.getSCL_QTY(handler) * line.getSCL_COST() * (1 - SC_C_DISCOUNT);
        return price;
    }

    public float getSC_TOTAL(TxnHandle handler) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        float totalWithoutTax = getSC_SHIP_COST(handler) + getSC_SUB_TOTAL(handler);
        return totalWithoutTax + (totalWithoutTax * 0.025f);
    }

    public String getSC_C_FNAME() {
        return SC_C_FNAME;
    }

    public void setSC_C_FNAME(String SC_C_FNAME) {
        this.SC_C_FNAME = SC_C_FNAME;
    }

    public String getSC_C_LNAME() {
        return SC_C_LNAME;
    }

    protected void setSC_C_LNAME(String SC_C_LNAME) {
        this.SC_C_LNAME = SC_C_LNAME;
    }

    public float getSC_C_DISCOUNT() {
        return SC_C_DISCOUNT;
    }

    public void setSC_C_DISCOUNT(float SC_C_DISCOUNT) {
        this.SC_C_DISCOUNT = SC_C_DISCOUNT;
    }

    @Override
    public Object copy() {
        ShoppingCart newCart = new ShoppingCart(SC_C_ID, SC_DATE, SC_C_FNAME, SC_C_LNAME, SC_C_DISCOUNT);
        return newCart;
    }

}

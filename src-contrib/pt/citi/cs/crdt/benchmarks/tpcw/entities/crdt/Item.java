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

import java.util.Date;
import java.util.TreeMap;

import org.uminho.gsd.benchmarks.interfaces.Entity;

import pt.citi.cs.crdt.benchmarks.tpcw.entities.TPCWNamingScheme;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class Item implements Copyable, Entity {

    private int I_ID;
    private String I_TITLE;
    private long I_PUB_DATE;
    private int I_A_ID;
    private String I_PUBLISHER;
    private String I_DESC;
    private String I_SUBJECT;
    private String I_THUMBNAIL;
    private String I_IMAGE;
    private double I_COST;
    private String I_ISBN;// international id
    private double I_SRP;// Suggested Retail Price
    private int I_RELATED1;
    private int I_RELATED2;
    private int I_RELATED3;
    private int I_RELATED4;
    private int I_RELATED5;
    private int I_PAGE;
    private long I_AVAIL; // Data when available
    private String I_BACKING;
    private String I_DIMENSION;

    private CRDTIdentifier I_STOCK;
    private CRDTIdentifier I_TOTAL_SOLD;

    Item() {
    }

    public Item(Integer i_id, String I_TITLE, long pubDate, String I_PUBLISHER, String I_DESC, String I_SUBJECT,
            String thumbnail, String image, double I_COST, String isbn, double srp, int I_RELATED1, int I_RELATED2,
            int I_RELATED3, int I_RELATED4, int I_RELATED5, int I_PAGE, long avail, String I_BACKING,
            String dimensions, int author) {
        this.I_ID = i_id;
        this.I_TITLE = I_TITLE;
        this.I_PUB_DATE = pubDate;
        this.I_A_ID = author;
        this.I_PUBLISHER = I_PUBLISHER;
        this.I_DESC = I_DESC;
        this.I_SUBJECT = I_SUBJECT;
        this.I_THUMBNAIL = thumbnail;
        this.I_IMAGE = image;
        this.I_COST = I_COST;
        this.I_ISBN = isbn;
        this.I_SRP = srp;
        this.I_RELATED1 = I_RELATED1;
        this.I_RELATED2 = I_RELATED2;
        this.I_RELATED3 = I_RELATED3;
        this.I_RELATED4 = I_RELATED4;
        this.I_RELATED5 = I_RELATED5;
        this.I_PAGE = I_PAGE;
        this.I_AVAIL = avail;
        this.I_BACKING = I_BACKING;
        this.I_DIMENSION = dimensions;

        this.I_STOCK = TPCWNamingScheme.forItemStock(i_id + "");
        this.I_TOTAL_SOLD = TPCWNamingScheme.forItemSold(i_id);

    }

    public int getI_AUTHOR() {
        return I_A_ID;
    }

    public String getI_BACKING() {
        return I_BACKING;
    }

    public double getI_COST() {
        return I_COST;
    }

    public String getI_DESC() {
        return I_DESC;
    }

    public int getI_PAGE() {
        return I_PAGE;
    }

    public String getI_PUBLISHER() {
        return I_PUBLISHER;
    }

    public String getI_SUBJECT() {
        return I_SUBJECT;
    }

    public String getI_TITLE() {
        return I_TITLE;
    }

    public Date getAvail() {
        return new Date(I_AVAIL);
    }

    public String getDimensions() {
        return I_DIMENSION;
    }

    public String getImage() {
        return I_IMAGE;
    }

    public String getIsbn() {
        return I_ISBN;
    }

    public Date getPubDate() {
        return new Date(I_PUB_DATE);
    }

    public double getI_SRP() {
        return I_SRP;
    }

    public String getThumbnail() {
        return I_THUMBNAIL;
    }

    public String getKeyName() {
        return "I_ID";
    }

    public Date getI_PUB_DATE() {
        return new Date(I_PUB_DATE);
    }

    public int getI_ID() {
        return I_ID;
    }

    public int getI_A_ID() {
        return I_A_ID;
    }

    public String getI_THUMBNAIL() {
        return I_THUMBNAIL;
    }

    public String getI_IMAGE() {
        return I_IMAGE;
    }

    public String getI_ISBN() {
        return I_ISBN;
    }

    public int getI_RELATED1() {
        return I_RELATED1;
    }

    public int getI_RELATED2() {
        return I_RELATED2;
    }

    public int getI_RELATED3() {
        return I_RELATED3;
    }

    public int getI_RELATED4() {
        return I_RELATED4;
    }

    public int getI_RELATED5() {
        return I_RELATED5;
    }

    public Date getI_AVAIL() {
        return new Date(I_AVAIL);
    }

    public String getI_DIMENSION() {
        return I_DIMENSION;
    }

    public int getI_TOTAL_SOLD(TxnHandle txn) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        IntegerTxnLocal stock = (IntegerTxnLocal) txn.get(I_TOTAL_SOLD, true, IntegerVersioned.class);
        if (stock.getValue() == null)
            stock.add(0);
        return stock.getValue();
    }

    public void addI_TOTAL_SOLD(int i_TOTAL_SOLD, TxnHandle txn) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        IntegerTxnLocal stock = (IntegerTxnLocal) txn.get(I_TOTAL_SOLD, true, IntegerVersioned.class);
        stock.add(i_TOTAL_SOLD);
    }

    public int getI_STOCK(TxnHandle txn/* , ObjectUpdatesListener updateListener */) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        IntegerTxnLocal stock = (IntegerTxnLocal) txn.get(I_STOCK, true, IntegerVersioned.class/*
                                                                                                * ,
                                                                                                * updateListener
                                                                                                */);
        return stock.getValue();
    }

    public void addI_STOCK(int i_STOCK, TxnHandle txn) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        IntegerTxnLocal stock = (IntegerTxnLocal) txn.get(I_STOCK, true, IntegerVersioned.class);
        stock.add(i_STOCK);
    }

    @Override
    public Object copy() {
        return new Item(I_ID, I_TITLE, I_PUB_DATE, I_PUBLISHER, I_DESC, I_SUBJECT, I_THUMBNAIL, I_IMAGE, I_COST,
                I_ISBN, I_SRP, I_RELATED1, I_RELATED2, I_RELATED3, I_RELATED4, I_RELATED5, I_PAGE, I_AVAIL, I_BACKING,
                I_DIMENSION, I_A_ID);
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("I_TITLE", I_TITLE);
        values.put("I_A_ID", I_A_ID);
        values.put("I_PUB_DATE", I_PUB_DATE);
        values.put("I_PUBLISHER", I_PUBLISHER);
        values.put("I_SUBJECT", I_SUBJECT);
        values.put("I_DESC", I_DESC);
        values.put("I_RELATED1", I_RELATED1);
        values.put("I_RELATED2", I_RELATED2);
        values.put("I_RELATED3", I_RELATED3);
        values.put("I_RELATED4", I_RELATED4);
        values.put("I_RELATED5", I_RELATED5);
        values.put("I_THUMBNAIL", I_THUMBNAIL);
        values.put("I_IMAGE", I_IMAGE);
        values.put("I_SRP", I_SRP);
        values.put("I_COST", I_COST);
        values.put("I_AVAIL", I_AVAIL);
        values.put("I_STOCK", I_STOCK);
        values.put("I_ISBN", I_ISBN);
        values.put("I_PAGE", I_PAGE);
        values.put("I_BACKING", I_BACKING);
        values.put("I_DIMENSION", I_DIMENSION);
        values.put("I_ID", I_ID);

        return values;
    }

    public void setI_PUB_DATE(long i_PUB_DATE) {
        I_PUB_DATE = i_PUB_DATE;
    }

    public void setI_RELATED1(int i_RELATED1) {
        I_RELATED1 = i_RELATED1;
    }

    public void setI_RELATED2(int i_RELATED2) {
        I_RELATED2 = i_RELATED2;
    }

    public void setI_RELATED3(int i_RELATED3) {
        I_RELATED3 = i_RELATED3;
    }

    public void setI_RELATED4(int i_RELATED4) {
        I_RELATED4 = i_RELATED4;
    }

    public void setI_RELATED5(int i_RELATED5) {
        I_RELATED5 = i_RELATED5;
    }

    public void setI_THUMBNAIL(String i_THUMBNAIL) {
        I_THUMBNAIL = i_THUMBNAIL;
    }

    public void setI_IMAGE(String i_IMAGE) {
        I_IMAGE = i_IMAGE;
    }

}
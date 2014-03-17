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

import swift.crdt.core.CRDTIdentifier;

/**
 * Provides methods for generating CRDT Identifiers based on the class and type
 * of object.
 * 
 * @author v.sousa
 */

public class TPCWNamingScheme {

    private static final String ItemsTableName = "items";
    private static final String ItemsSoldTableName = "itemsSold";
    private static final String ItemsStockTableName = "itemsStock";
    private static String shoppingCartTableName = "shoppingCarts";
    private static String shoppingCartItemsTableName = "shoppingCartsItems";
    private static String scLinesTableName = "scLines";
    private static String ordersTableName = "orders";
    private static String orderLinesTableName = "orderLines";
    private static String countryTableName = "country";
    private static final String addressTableName = "addresses";
    private static final String bestSellersTableName = "bestSellers";
    private static final String customerTableName = "customers";
    private static final String customerCartTableName = "customersCarts";
    private static final String CCXactItemTableName = "ccxactitem";
    private static String orderPriceTableName = "orderPrice";
    private static String authorTableName = "authors";
    private static String indexTableName = "indexes";
    private static String ordersIndex = "ordersOrdered";

    /**
     * Generates a CRDT identifier for the available stock of an item from its
     * identifier.
     * 
     * @param itemID
     *            identifier
     * @return CRDT identifier for item stock
     */
    public static CRDTIdentifier forItemStock(final String itemID) {
        return new CRDTIdentifier(ItemsStockTableName, itemID);
    }

    /**
     * Generates a CRDT identifier for an item's sales.
     * 
     * @param itemID
     *            identifier
     * @return CRDT identifier for an item's sales
     */
    public static CRDTIdentifier forItemSold(final int itemID) {
        return new CRDTIdentifier(ItemsSoldTableName, itemID + "");
    }

    public static CRDTIdentifier forItem(final String itemID) {
        return new CRDTIdentifier(ItemsTableName, itemID);
    }

    public static CRDTIdentifier forSCLine(String itemID, String shoppingCartID) {
        return new CRDTIdentifier(scLinesTableName, itemID + "_" + shoppingCartID);
    }

    public static CRDTIdentifier forShoppingCart(String user_id) {
        return new CRDTIdentifier(shoppingCartTableName, user_id);
    }

    public static CRDTIdentifier forOrder(String user_id) {
        return new CRDTIdentifier(ordersTableName, user_id);
    }

    public static CRDTIdentifier forOrderLines(String user_id) {
        return new CRDTIdentifier(orderLinesTableName, user_id);
    }

    public static CRDTIdentifier forAddress(String address) {
        return new CRDTIdentifier(addressTableName, address);
    }

    // public static CRDTIdentifier forBestSellersBuffer(String entry) {
    // return new CRDTIdentifier(bestSellersBufferTableName, entry /*
    // * + "_" +
    // * counter.
    // * incrementAndGet
    // * ()
    // */);
    // }

    public static CRDTIdentifier forBestSellers(String subject) {
        return new CRDTIdentifier(bestSellersTableName, subject);
    }

    public static CRDTIdentifier forCCXactItem(String order_id) {
        return new CRDTIdentifier(CCXactItemTableName, order_id);
    }

    public static CRDTIdentifier forCountry(String country_id) {
        return new CRDTIdentifier(countryTableName, country_id);
    }

    public static CRDTIdentifier forCustomer(String customer_id) {
        return new CRDTIdentifier(customerTableName, customer_id);
    }

    public static CRDTIdentifier forAuthor(String author_id) {
        return new CRDTIdentifier(authorTableName, author_id);
    }

    public static CRDTIdentifier forShoppingCartItems(String cart_id) {
        return new CRDTIdentifier(shoppingCartItemsTableName, cart_id);
    }

    public static String getItemsTableName() {
        return ItemsTableName;
    }

    public static String getItemsStockTableName() {
        return ItemsStockTableName;
    }

    public static String getShoppingCartTableName() {
        return shoppingCartTableName;
    }

    public static String getItemsSoldTableName() {
        return ItemsSoldTableName;
    }

    public static String getCountryTableName() {
        return countryTableName;
    }

    // public static String getBestSellersBufferTableName() {
    // return bestSellersBufferTableName;
    // }

    public static String getBestSellersTableName() {
        return bestSellersTableName;
    }

    public static String getAuthorTableName() {
        return authorTableName;
    }

    public static String getOrdersTableName() {
        return ordersTableName;
    }

    public static String getCustomersTableName() {
        return customerTableName;
    }

    public static String getAddressTableName() {
        return addressTableName;
    }

    public static String getShoppingCartItemsTableName() {
        return shoppingCartItemsTableName;
    }

    public static CRDTIdentifier forIndex(String index) {
        return new CRDTIdentifier(indexTableName, index);
    }

    public static CRDTIdentifier forOrderPrice(String O_ID) {
        return new CRDTIdentifier(orderPriceTableName, O_ID);
    }

    public static CRDTIdentifier forCustomerLastCart(String c_ID) {
        return new CRDTIdentifier(customerCartTableName, c_ID);
    }

    public static CRDTIdentifier forOrdersIndex() {
        return new CRDTIdentifier(indexTableName, ordersIndex);
    }

}

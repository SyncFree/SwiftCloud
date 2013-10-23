/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */
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
package pt.citi.cs.crdt.benchmarks.tpcw.database;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.uminho.gsd.benchmarks.benchmark.BenchmarkMain;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkNodeID;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.generic.BuyingResult;
import org.uminho.gsd.benchmarks.generic.Constants;
import org.uminho.gsd.benchmarks.helpers.BenchmarkUtil;
import org.uminho.gsd.benchmarks.helpers.TPM_counter;
import org.uminho.gsd.benchmarks.helpers.ThinkTime;
import org.uminho.gsd.benchmarks.interfaces.Entity;
import org.uminho.gsd.benchmarks.interfaces.KeyGenerator;
import org.uminho.gsd.benchmarks.interfaces.Workload.Operation;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import pt.citi.cs.crdt.benchmarks.tpcw.entities.Address;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.Author;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.AuthorIndex;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.BestSellerEntry;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.CCXactItem;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.Country;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.Customer;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.TPCWNamingScheme;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.Item;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.Order;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.OrderInfo;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.OrderLine;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.SCLine;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.SetAuthorIndex;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.SetBestSellers;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.SetIndexByDate;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.SetTxnLocalAuthorIndex;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.SetTxnLocalBestSellers;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.SetTxnLocalIndexByDate;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.ShoppingCart;
import pt.citi.cs.crdt.benchmarks.tpcw.synchronization.TPCWRpc;
import swift.client.CommitListener;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.SetStrings;
import swift.crdt.SetTxnLocalString;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.SwiftScout;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.exceptions.CvRDTSerializationException;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.Pair;
import sys.Sys;
import sys.net.api.rpc.RpcHandle;
import ds.tree.RadixTreeImpl;

public class TPCW_SwiftCloud_Executor implements DatabaseExecutorInterface {

    public static final int RECORD_SEARCH_LIMIT = 50;
    public static final int NUM_ORDERS_ADMIN = 1000;
    private static final boolean SIMULATED_CLIENT = true;
    private KeyGenerator keyGenerator;
    private TPM_counter counter;
    private long simulatedDelay;
    private int global_executor_counter;
    private static AtomicInteger atomic_global_executor_counter = new AtomicInteger();
    private int executor_id;
    private ResultHandler client_result_handler;

    // TODO: Quick Hack
    private AtomicInteger another_executor_id = new AtomicInteger();

    private static RadixTreeImpl<List<String>> authorFirstNameSearchIndex = new RadixTreeImpl<List<String>>();
    private static RadixTreeImpl<List<String>> authorMiddleNameSearchIndex = new RadixTreeImpl<List<String>>();
    private static RadixTreeImpl<List<String>> authorLastNameSearchIndex = new RadixTreeImpl<List<String>>();
    private static RadixTreeImpl<String> titleSearchIndex = new RadixTreeImpl<String>();
    private static Map<String, List<String>> subjectSearchIndex = new HashMap<String, List<String>>();

    IsolationLevel ISOLATION_LEVEL;
    CachePolicy CACHE_POLICY;

    // Must be static to be shared among concurrent threads
    public static SwiftScout scout;
    private static boolean stopped = false;

    TPCWCommitListener commitListener = new TPCWCommitListener();
    /* ObjectUpdatesListener updateListener = new TPCWUpdateListener(); */
    ThreadLocal<TxnHandle> threadTransaction = new ThreadLocal<TxnHandle>();

    Map<String, Integer> partialBought = new TreeMap<String, Integer>();
    ArrayList<String> operations = new ArrayList<String>();
    int bought_qty;
    int bought_actions;
    int bought_carts;
    int zeros;
    int last = 0, lastRaw = 0, lastSwift = 0;

    // private String[] credit_cards = { "VISA", "MASTERCARD", "DISCOVER",
    // "AMEX", "DINERS" };
    private String[] ship_types = { "AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL" };
    private String[] status_types = { "PROCESSING", "SHIPPED", "PENDING", "DENIED" };

    private Random random = new Random();
    private Queue<Pair<RpcHandle, TPCWRpc>> requestsQueue;
    private boolean finished;
    private SwiftSession localSession;
    private static BufferedWriter bufferedOutput;

    public TPCW_SwiftCloud_Executor(String keyspace, Map<String, Integer> connections, Map<String, String> key_paths,
            int think_time, int search_slices, KeyGenerator keyGenerator, TPM_counter tpm_counter,
            String isolationLevel, String cachePolicy, Queue<Pair<RpcHandle, TPCWRpc>> requestsQueue) {

        this.requestsQueue = requestsQueue;
        this.keyGenerator = keyGenerator;
        this.counter = tpm_counter;
        
        this.ISOLATION_LEVEL = IsolationLevel.valueOf(isolationLevel);
        this.CACHE_POLICY = CachePolicy.valueOf(cachePolicy);
        // this.ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED;

        simulatedDelay = think_time;

        if (scout == null || stopped) {

            Sys.init();

            // Only one scout entry is created
            for (String rand_host : connections.keySet()) {
                String host = rand_host.split(":")[0];

                SwiftOptions options = new SwiftOptions(host, connections.get(rand_host));
                options.setConcurrentOpenTransactions(false);

                options.setCacheSize(30000);
                options.setMaxCommitBatchSize(50);
                options.setMaxAsyncTransactionsQueued(10);
                options.setDeadlineMillis(240 * 1000);
                options.setCacheEvictionTimeMillis(60 * 1000 * 20);
                scout = SwiftImpl.newMultiSessionInstance(options);
                stopped = false;
                break;
            }

        }

        localSession = scout.newSession(BenchmarkMain.swiftCloudNodeID + "_" + another_executor_id.getAndIncrement());
            }

    @Override
    public void start(WorkloadGeneratorInterface workload, BenchmarkNodeID nodeId, int operation_number,
            ResultHandler handler) {

        global_executor_counter++;
        executor_id = global_executor_counter;
        atomic_global_executor_counter.incrementAndGet();
        client_result_handler = handler;

        for (int operation = 0; operation < operation_number; operation++) {
            long g_init_time = System.currentTimeMillis();

            try {
                long init_time = System.currentTimeMillis();

                Operation op = workload.getNextOperation();
                executeMethod(op);
                long end_time = System.currentTimeMillis();
                client_result_handler.logResult(op.getOperation(), (end_time - init_time));

                simulatedDelay = ThinkTime.getThinkTime();

                if (simulatedDelay > 0) {
                    Thread.sleep(simulatedDelay);
                }

            } catch (NoSuchFieldException e) {
                System.out.println("[ERROR:] THIS OPERATION DOES NOT EXIST: " + e.getMessage());
                break;
            } catch (InterruptedException e) {
                System.out.println("[ERROR:] THINK TIME AFTER METHOD EXECUTION INTERRUPTED: " + e.getMessage());
                break;

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("-- Error : Client " + executor_id + " going down....");
                break;

            }
            long end_time = System.currentTimeMillis();
            counter.increment();
            client_result_handler.logResult("OPERATIONS", (end_time - g_init_time));

        }

        finished = true;

        if (atomic_global_executor_counter.decrementAndGet() == 0) {
            scout.stop(true);
            stopped = true;
        }

        client_result_handler.getResulSet().put("bought", partialBought);
        client_result_handler.getResulSet().put("total_bought", bought_qty);
        client_result_handler.getResulSet().put("buying_actions", bought_actions);
        client_result_handler.getResulSet().put("bought_carts", bought_carts);
        client_result_handler.getResulSet().put("zeros", zeros);

    }

    public void execute(Operation op) throws Exception {
        executeMethod(op);
    }

    public void executeMethod(Operation op) throws Exception {

        Pair<RpcHandle, TPCWRpc> request;
        TPCWRpc message;
        if (SIMULATED_CLIENT) {
            while ((request = requestsQueue.poll()) == null) {
                Thread.sleep(50);
            }
        } else {
            message = new TPCWRpc(System.currentTimeMillis());
        }

        operations.add(op.getOperation());

        String method_name = op.getOperation();
        // list.add(method_name);
        // System.out.println(list);
        if (method_name.equalsIgnoreCase("GET_STOCK_AND_PRODUCTS")) {
            Map<String, Entity> items_info = getRangeCRDT(TPCWNamingScheme.getItemsTableName(), true, -1, null);
            op.setResult(items_info);

        } else if (method_name.equalsIgnoreCase("Get_Stock_And_Products_after_increment")) {

            ArrayList<String> fields = new ArrayList<String>();
            int stock = (Integer) op.getParameter("STOCK");
            setItemStocks(stock);

            System.out.println("sleeping after stock reposition...");
            Thread.sleep(60000);

            fields.add("I_STOCK");
            Map<String, Entity> items_info = getRangeCRDT(TPCWNamingScheme.getItemsTableName(), true, -1, null);
            op.setResult(items_info);

        } else if (method_name.equalsIgnoreCase("GET_ITEM_STOCK")) {

            String item_id = (String) op.getParameters().get("ITEM_ID");

            Object o = read(item_id, "item", "I_STOCK", null);
            int stock = -1;
            if (o != null) {
                stock = (Integer) o;
            }
            op.setResult(stock);

        } else if (method_name.equalsIgnoreCase("ADD_TO_CART")) {

            String cart = (String) op.getParameter("CART_ID");
            String item_id = (String) op.getParameter("ITEM_ID");
            int qty = (Integer) op.getParameter("QTY");

            addToCart(cart, item_id, qty);

        } else if (method_name.equalsIgnoreCase("BUY_CART")) {

            bought_carts++;
            String cart_id = (String) op.getParameter("CART_ID");

            Customer c = null;

            if (op.getParameters().containsKey("Customer")) {
                c = (Customer) op.getParameter("Customer");
            }
            buyCart(cart_id, c);

        } else if (method_name.equalsIgnoreCase("GET_BENCHMARK_RESULTS")) {
            // op.setResult(getResults());

        } else if (method_name.equalsIgnoreCase("OP_HOME")) {

            int costumer = (Integer) op.getParameter("COSTUMER");
            int item_id = (Integer) op.getParameter("ITEM");
            HomeOperation(costumer, item_id);
        } else if (method_name.equalsIgnoreCase("OP_SHOPPING_CART")) {

            String cart = (String) op.getParameter("CART");
            int item_id = (Integer) op.getParameter("ITEM");
            boolean create = (Boolean) op.getParameter("CREATE");
            shoppingCartInteraction(item_id, create, cart);

        } else if (method_name.equalsIgnoreCase("OP_REGISTER")) {
            String customer = (String) op.getParameter("CUSTOMER");

            CustomerRegistration(customer);

        } else if (method_name.equalsIgnoreCase("OP_LOGIN")) {

            String customer = (String) op.getParameter("CUSTOMER");

            refreshSession(customer);

        } else if (method_name.equalsIgnoreCase("OP_BUY_REQUEST")) {

            String id = (String) op.getParameter("CART");
            BuyRequest(id);

        } else if (method_name.equalsIgnoreCase("OP_BUY_CONFIRM")) {

            String car_id = (String) op.getParameter("CART");
            String costumer = (String) op.getParameter("CUSTOMER");
            BuyConfirm(costumer, car_id);

        } else if (method_name.equalsIgnoreCase("OP_ORDER_INQUIRY")) {
            String costumer = (String) op.getParameter("CUSTOMER");

            OrderInquiry(costumer);

        } else if (method_name.equalsIgnoreCase("OP_SEARCH")) {
            String term = (String) op.getParameter("TERM");
            String field = (String) op.getParameter("FIELD");
            doSearch(term, field);

        } else if (method_name.equalsIgnoreCase("OP_NEW_PRODUCTS")) {
            String field = (String) op.getParameter("FIELD");
            newProducts(field);

        } else if (method_name.equalsIgnoreCase("OP_BEST_SELLERS")) {
            String field = (String) op.getParameter("FIELD");
            BestSellers(field);
        } else if (method_name.equalsIgnoreCase("OP_ITEM_INFO")) {
            int id = (Integer) op.getParameter("ITEM");
            ItemInfo(id);
        } else if (method_name.equalsIgnoreCase("OP_ADMIN_CHANGE")) {
            int id = (Integer) op.getParameter("ITEM");
            AdminChange(id);

        } else if (method_name.equalsIgnoreCase("")) {

        } else {
            System.out.println("[WARN:]UNKNOWN REQUESTED METHOD: " + method_name);

        }

        if (SIMULATED_CLIENT) {
            request.getSecond().setOperation(System.currentTimeMillis(), op.getOperation(),
                    Thread.currentThread().getId());
            request.getFirst().reply(request.getSecond());
        } else {
            long time = System.currentTimeMillis();
            message.setOperation(time, op.getOperation(), Thread.currentThread().getId());
            message.setTotalTime(time);
            if (bufferedOutput != null)
                bufferedOutput.write(message.toString() + "\n");
        }

    }

    @Override
    public Object insert(String key, String bucketName, Entity value) throws NetworkException {
        TxnHandle txh = threadTransaction.get();
        RegisterTxnLocal<Entity> entity;
        try {
            entity = (RegisterTxnLocal<Entity>) txh.get(new CRDTIdentifier(bucketName, key), true,
                    RegisterVersioned.class);
            entity.set(value);
            SetTxnLocalString index = txh.get(TPCWNamingScheme.forIndex(bucketName), true, SetStrings.class);
            index.insert(key);
            return entity;
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }
        return value;
    }

    @Override
    public void remove(String key, String bucketName, String column) throws Exception {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);

        SetTxnLocalString index = handler.get(TPCWNamingScheme.forIndex(bucketName), false, SetStrings.class);
        index.remove(key);
        handler.commitAsync(commitListener);
    }

    public void update(String key, String bucketName, String column, Object value, String superfield) throws Exception {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
        RegisterTxnLocal<Entity> entityRegister = (RegisterTxnLocal<Entity>) handler.get(new CRDTIdentifier(bucketName,
                key), false, RegisterVersioned.class);
        insertOrModifyAttribute(entityRegister.getValue(), value, column);
        entityRegister.set(entityRegister.getValue());
        handler.commitAsync(commitListener);
    }

    public Object read(String key, String bucketName, String column, String superfield) throws Exception {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);
        RegisterTxnLocal<Entity> entityRegister = (RegisterTxnLocal<Entity>) handler.get(new CRDTIdentifier(bucketName,
                key), false, RegisterVersioned.class);
        Field fld = getEntityClass(bucketName).getField(column);
        return fld.get(entityRegister.getValue());

    }

    @Override
    public Map<String, Map<String, Object>> rangeQuery(String bucketName, List<String> fields, int limit)
            throws Exception {
        Map<String, Entity> crdtResult = getRangeCRDT(bucketName, true, limit, null);
        Map<String, Map<String, Object>> results = new HashMap<String, Map<String, Object>>(crdtResult.size());
        for (Entry<String, Entity> entry : crdtResult.entrySet()) {
            Map<String, Object> columnValues = new HashMap<String, Object>();
            for (Entry<String, Object> pair : entry.getValue().getValuesToInsert().entrySet()) {
                columnValues.put(pair.getKey(), pair.getValue());
            }
            results.put(entry.getKey(), columnValues);
        }
        return results;

    }

    public Map<String, Entity> getRecentOrders(int limit, CRDTIdentifier ordersSet, TxnHandle handler) {
        Map<String, Entity> results = new HashMap<String, Entity>(1000);
        int count = 0;
        boolean createdHandler = false;

        try {

            if (handler == null) {
                handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
                createdHandler = true;
            }

            SetTxnLocalIndexByDate keyIndexesCRDT = handler.get(ordersSet, false, SetIndexByDate.class);

            Collection<OrderInfo> listOfKeys = keyIndexesCRDT.getValue();

//            if (BULK_GET) {
//                Set<CRDTIdentifier> ids = new HashSet<CRDTIdentifier>();
//                for (OrderInfo key : listOfKeys) {
//                    if (count == limit)
//                        break;
//                    ids.add(TPCWNamingScheme.forOrder(key.getO_ID()));
//                    count++;
//                }
//                handler.bulkGet(ids, false, TIMEOUT_BULK_OP, null);
//            }

            count = 0;
            for (OrderInfo key : listOfKeys) {
                if (count == limit)
                    break;
                RegisterTxnLocal<Entity> entity = (RegisterTxnLocal<Entity>) handler.get(
                        TPCWNamingScheme.forOrder(key.getO_ID()), false, RegisterVersioned.class);
                results.put(key.getO_ID(), entity.getValue());
                count++;
            }
            if (createdHandler)
                handler.commitAsync(commitListener);
            return results;
        } catch (NetworkException e) {
            e.printStackTrace();
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private synchronized <T extends RegisterVersioned<RegisterVersioned<Entity>>> Map<String, Entity> getRangeCRDT(
            String bucketName, boolean readOnly, int limit, TxnHandle handler) {

        Map<String, Entity> results = new HashMap<String, Entity>();
        int count = 0;
        boolean createdHandler = false;

        try {

            if (handler == null) {
                handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, readOnly);
                createdHandler = true;
            }

            SetTxnLocalString keyIndexesCRDT = handler.get(TPCWNamingScheme.forIndex(bucketName), false,
                    SetStrings.class);

            Set<String> stringIDs = keyIndexesCRDT.getValue();
//            if (BULK_GET) {
//                Set<CRDTIdentifier> crdtIDs = new HashSet<CRDTIdentifier>();
//                for (String s : stringIDs) {
//                    if (count == limit)
//                        break;
//                    crdtIDs.add(new CRDTIdentifier(bucketName, s));
//                    count++;
//                }
//
//                handler.bulkGet(crdtIDs, false, TIMEOUT_BULK_OP, null);
//            }
            count = 0;
            for (String key : stringIDs) {
                if (count == limit)
                    break;
                RegisterTxnLocal<Entity> entity = (RegisterTxnLocal<Entity>) handler.get(new CRDTIdentifier(bucketName,
                        key), false, RegisterVersioned.class);
                results.put(key, entity.getValue());
                count++;
            }
            if (createdHandler)
                handler.commitAsync(commitListener);
            return results;
        } catch (NetworkException e) {
            e.printStackTrace();
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void truncate(String bucketName) throws Exception {
        // TODO: Needs to be implemented if we want to maintain the dataset
        // between runs of the benchmark
    }

    @Override
    public void index(String key, String path, Object value) throws Exception {

    }

    @Override
    public void index(String key, String path, String indexed_key, Map<String, Object> value) throws Exception {

    }

    public void index(String key, String path, String indexed_key, AuthorIndex entity) throws Exception {

    }

    public void closeScout() {
        scout.stop(true);
        stopped = true;
    }

    @Override
    public void closeClient() {
    }

    @Override
    public Map<String, String> getInfo() {
        TreeMap<String, String> info = new TreeMap<String, String>();
        return info;
    }

    /********************************************************/
    /**** TPCW benchmark consistency and old operations ****/
    /**
     * ****************************************************
     */

    public void setItemStocks(int initial_stock) throws Exception {

        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);

        SetTxnLocalString keyIndexesCRDT = handler.get(TPCWNamingScheme.forIndex(TPCWNamingScheme.getItemsTableName()),
                false, SetStrings.class/*
                                        * , updateListener
                                        */);

        for (String key : keyIndexesCRDT.getValue()) {
            IntegerTxnLocal stock = handler.get(TPCWNamingScheme.forItemStock(key), false, IntegerVersioned.class/*
                                                                                                                  * ,
                                                                                                                  * updateListener
                                                                                                                  */);

            stock.add(-stock.getValue() + initial_stock);

        }
        handler.commitAsync(commitListener);
    }

    /**
     * Adds a quantity of a specific item to a cart
     * 
     * @param cart
     *            the cart id
     * @param item
     *            the item id
     * @param qty_to_add
     *            the quantity to add
     * @throws Exception
     */
    public void addToCart(String cart, String item, int qty_to_add) throws Exception {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);

        RegisterTxnLocal<ShoppingCart> shoppingCart = (RegisterTxnLocal<ShoppingCart>) handler.get(
                TPCWNamingScheme.forShoppingCart(cart), false, RegisterVersioned.class);
        ShoppingCart sc = shoppingCart.getValue();

        SCLine sc_line = sc.getSCLine(item, handler);
        sc_line.addSCL_QTY(qty_to_add, handler);
        handler.commitAsync(commitListener);

    }

    /**
     * Updates the stock an the quantity sold of an item
     * 
     * @param item
     *            the id of the item being updated
     * @param qty
     *            the quantity of the item being updated
     * @param handler2
     * @return the state of the buy operation
     * @throws NetworkException
     */
    private BuyingResult BuyCartItem(String item, int qty, TxnHandle handler) throws NetworkException {
        RegisterTxnLocal<Item> itemRegister;
        try {
            itemRegister = (RegisterTxnLocal<Item>) handler.get(TPCWNamingScheme.forItem(item), false,
                    RegisterVersioned.class);

            if (itemRegister.getValue() == null) {
                return BuyingResult.DOES_NOT_EXIST;
            }
            Item itemObj = itemRegister.getValue();
            int stock = itemObj.getI_STOCK(handler);
            if ((stock - qty) >= 0) {
                if (stock - qty == 0) {
                    System.out.println("NOT enough STOCk");
                    zeros++;
                }
                stock -= qty;
                itemObj.addI_STOCK(-qty, handler);
                if (itemObj.getI_STOCK(handler) < 10)
                    itemObj.addI_STOCK(21, handler);
                itemObj.addI_TOTAL_SOLD(qty, handler);

                updateBestSeller(itemObj, handler);

            } else {
                System.out.println("NOT enough STOCk");
                return BuyingResult.NOT_AVAILABLE;
            }
            return BuyingResult.BOUGHT;

        } catch (WrongTypeException e) {
            e.printStackTrace();
            return BuyingResult.CANT_COMFIRM;
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
            return BuyingResult.CANT_COMFIRM;
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
            return BuyingResult.CANT_COMFIRM;
        }

    }

    /**
     * Buy a specific item
     * 
     * @param item_id
     *            the item being bought
     * @param qty
     *            the quantity being bought
     * @param handler
     * @return the state of the buy operation
     * @throws NetworkException
     */
    private BuyingResult buyItem(String item_id, int qty, TxnHandle handler) throws NetworkException {
        long init_time = System.currentTimeMillis();
        BuyingResult result = BuyCartItem(item_id, qty, handler);
        long end_time = System.currentTimeMillis();
        client_result_handler.logResult("BUY_ITEM", (end_time - init_time));
        List<Object> buying_information = new LinkedList<Object>();
        buying_information.add(qty);
        buying_information.add(init_time);
        buying_information.add(end_time);
        client_result_handler.record_unstructured_data("BOUGHT_ITEMS_TIMELINE", item_id + "", buying_information);

        return result;
    }

    /**
     * Buys the items of a shopping cart
     * 
     * @param cart_id
     *            the id of the shopping cart
     * @param c
     *            the costumer executing the operation
     */
    private void buyCart(String cart_id, Customer c) {
        try {
            TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
            RegisterTxnLocal<ShoppingCart> shop_c;
            try {
                shop_c = (RegisterTxnLocal<ShoppingCart>) handler.get(TPCWNamingScheme.forShoppingCart(cart_id), false,
                        RegisterVersioned.class);

                Collection<SCLine> scLines = shop_c.getValue().getSCLines(handler);
                for (SCLine line : scLines) {
                    int qty = line.getSCL_QTY(handler);
                    BuyingResult result = buyItem(line.getI_ID() + "", qty, handler);
                    client_result_handler.countEvent("BUYING_COUNTERS", result.name(), 1);

                    if (result.equals(BuyingResult.BOUGHT)) {
                        bought_qty += qty;
                        bought_actions++;
                        if (!partialBought.containsKey(line.getI_ID())) {
                            partialBought.put(line.getI_ID() + "", qty);

                        } else {
                            int bought = partialBought.get(line.getI_ID() + "");
                            partialBought.put(line.getI_ID() + "", (qty + bought));
                        }

                    }
                }
            } catch (VersionNotFoundException e) {
                e.printStackTrace();
            }
            handler.commit();
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (NetworkException e) {
            e.printStackTrace();
        }

    }

    /**************************************/
    /**** TPCW benchmark operations ****/
    /**
     * ********************************
     */

    /**
     * Shows information about the user and a specific item
     * 
     * @param customer
     *            the costumer id
     * @param item
     *            the item id
     * @throws Exception
     */
    public void HomeOperation(int customer, int item) throws Exception {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);
        RegisterTxnLocal<Item> itemCRDT = (RegisterTxnLocal<Item>) handler.get(TPCWNamingScheme.forItem(item + ""),
                false, RegisterVersioned.class);
        Item itemObj = itemCRDT.getValue();
        RegisterTxnLocal<Customer> customerCRDT = (RegisterTxnLocal<Customer>) handler.get(
                TPCWNamingScheme.forCustomer(customer + ""), false, RegisterVersioned.class);
        Customer customerObj = customerCRDT.getValue();
        handler.commitAsync(commitListener);
    }

    /**
     * Adds an item to the shopping cart (Note: The quantity added is one)
     * 
     * @param item
     *            the item being added.
     * @param create
     *            true if it is necessary to create the shopping cart
     * @param cart_id
     *            the shopping cart id
     * @throws NetworkException
     */
    public void shoppingCartInteraction(int item, boolean create, String cart_id) throws NetworkException {
        ShoppingCart shop_c;
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
        try {
            if (create) {
                Timestamp stamp = new Timestamp(System.currentTimeMillis());
                shop_c = new ShoppingCart(cart_id);
                shop_c.setSC_DATE(stamp.toString());
                RegisterTxnLocal<ShoppingCart> shopCRDT;
                shopCRDT = (RegisterTxnLocal<ShoppingCart>) handler.get(TPCWNamingScheme.forShoppingCart(cart_id),
                        true, RegisterVersioned.class);

                shopCRDT.set(shop_c);

            } else {

                RegisterTxnLocal<ShoppingCart> shopCRDT = (RegisterTxnLocal<ShoppingCart>) handler.get(
                        TPCWNamingScheme.forShoppingCart(cart_id), false, RegisterVersioned.class);
                shop_c = shopCRDT.getValue();

            }

            RegisterTxnLocal<Item> itemCRDT = (RegisterTxnLocal<Item>) handler.get(TPCWNamingScheme.forItem(item + ""),
                    false, RegisterVersioned.class);
            Item itemObj = itemCRDT.getValue();

            SCLine sc_line = shop_c.getSCLine(item + "", handler);

            if (sc_line != null) {
                sc_line.addSCL_QTY(1, handler);
            } else {
                sc_line = new SCLine(item, shop_c.getSC_C_ID());
                sc_line.setSCL_COST(itemObj.getI_COST());
                sc_line.setSCL_SRP(itemObj.getI_SRP());
                sc_line.setSCL_TITLE(itemObj.getI_TITLE());
                sc_line.setSCL_BACKING(itemObj.getI_BACKING());
                sc_line.addSCL_QTY(1, handler);
                sc_line.setI_ID(itemObj.getI_ID());
                shop_c.addSCLine(sc_line, handler);
            }

        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

        handler.commit();

    }

    // TODO: confirm if specification creates new address, when it does not
    // exist
    /**
     * Registers a new user in the system
     * 
     * @param costumer_id
     *            the id of the costumer
     * @throws NetworkException
     */
    public void CustomerRegistration(String costumer_id) throws NetworkException {

        String name = (BenchmarkUtil.getRandomAString(8, 13) + " " + BenchmarkUtil.getRandomAString(8, 15));
        String[] names = name.split(" ");
        Random r = new Random();
        int random_int = r.nextInt(1000);

        String key = names[0] + "_" + (costumer_id);

        String pass = names[0].charAt(0) + names[1].charAt(0) + "" + random_int;

        String first_name = names[0];

        String last_name = names[1];

        int phone = r.nextInt(999999999 - 100000000) + 100000000;

        String email = key + "@" + BenchmarkUtil.getRandomAString(2, 9) + ".com";

        double discount = r.nextDouble();

        String adress = "Street: "
                + (BenchmarkUtil.getRandomAString(8, 15) + " " + BenchmarkUtil.getRandomAString(8, 15)) + " number: "
                + r.nextInt(500);

        double C_BALANCE = 0.00;

        double C_YTD_PMT = (double) BenchmarkUtil.getRandomInt(0, 99999) / 100.0;

        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, -1 * BenchmarkUtil.getRandomInt(1, 730));

        java.sql.Date C_SINCE = new java.sql.Date(cal.getTime().getTime());

        cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(0, 60));
        if (cal.after(new GregorianCalendar())) {
            cal = new GregorianCalendar();
        }

        java.sql.Date C_LAST_LOGIN = new java.sql.Date(cal.getTime().getTime());

        java.sql.Timestamp C_LOGIN = new java.sql.Timestamp(System.currentTimeMillis());

        cal = new GregorianCalendar();
        cal.add(Calendar.HOUR, 2);

        java.sql.Timestamp C_EXPIRATION = new java.sql.Timestamp(cal.getTime().getTime());

        cal = BenchmarkUtil.getRandomDate(1880, 2000);
        java.sql.Date C_BIRTHDATE = new java.sql.Date(cal.getTime().getTime());

        String C_DATA = BenchmarkUtil.getRandomAString(100, 500);

        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
        String address_id = insertAdress(handler);

        Customer c = new Customer(costumer_id, key, pass, last_name, first_name, phone + "", email, C_SINCE.toString(),
                C_LAST_LOGIN.toString(), C_LOGIN.toString(), C_EXPIRATION.toString(), C_BALANCE, C_YTD_PMT,
                C_BIRTHDATE.toString(), C_DATA, discount, address_id);

        threadTransaction.set(handler);
        insert(costumer_id, TPCWNamingScheme.getCustomersTableName(), c);

        handler.commitAsync(commitListener);
    }

    public String insertAdress(TxnHandle handler) {

        String ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE;
        String ADDR_ZIP;
        int country_id;

        ADDR_STREET1 = "street" + BenchmarkUtil.getRandomAString(10, 30);

        ADDR_STREET2 = "street" + BenchmarkUtil.getRandomAString(10, 30);
        ADDR_CITY = BenchmarkUtil.getRandomAString(4, 30);
        ADDR_STATE = BenchmarkUtil.getRandomAString(2, 20);
        ADDR_ZIP = BenchmarkUtil.getRandomAString(5, 10);
        country_id = BenchmarkUtil.getRandomInt(0, 92 - 1);

        String key = ADDR_STREET1 + ADDR_STREET2 + ADDR_CITY + ADDR_STATE + ADDR_ZIP + country_id;

        pt.citi.cs.crdt.benchmarks.tpcw.entities.Address address = new Address(key, ADDR_STREET1, ADDR_STREET2,
                ADDR_CITY, ADDR_STATE, ADDR_ZIP, country_id);

        RegisterTxnLocal<Address> storedAddress;
        try {
            storedAddress = (RegisterTxnLocal<Address>) handler.get(TPCWNamingScheme.forAddress(key), true,
                    RegisterVersioned.class);
        } catch (WrongTypeException e) {
            e.printStackTrace();
            return key;
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
            return key;
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
            return key;
        } catch (NetworkException e) {
            e.printStackTrace();
            return key;
        }
        if (storedAddress == null) {
            storedAddress.set(address);

        }

        return key;
    }

    // Not sure if this is being used
    /**
     * Refresh the client's session
     * 
     * @param C_ID
     *            the costumer id
     * @throws Exception
     */
    public void refreshSession(String C_ID) throws Exception {

        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
        RegisterTxnLocal<Customer> customerCRDT = (RegisterTxnLocal<Customer>) handler.get(
                TPCWNamingScheme.forCustomer(C_ID), false, RegisterVersioned.class);
        Customer customer = customerCRDT.getValue();
        RegisterTxnLocal<Address> addressCRDT = (RegisterTxnLocal<Address>) handler.get(
                TPCWNamingScheme.forAddress(customer.getAddress()), false, RegisterVersioned.class);
        Address address = addressCRDT.getValue();
        customer.setLogin(new Timestamp(System.currentTimeMillis()).toString());
        customer.setExpiration((new Timestamp(System.currentTimeMillis() + 7200000)).toString());

        customerCRDT.set(customer);
        handler.commitAsync(commitListener);

    }

    /**
     * Computes the order info (Method implementation slightly different from
     * specification)
     * 
     * @param shopping_id
     *            the shopping cart id
     * @throws IOException
     * @throws CvRDTSerializationException
     * @throws SwiftException
     */
    public void BuyRequest(String shopping_id) throws IOException, CvRDTSerializationException, SwiftException {

        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
        RegisterTxnLocal<ShoppingCart> shoppingCart = (RegisterTxnLocal<ShoppingCart>) handler.get(
                TPCWNamingScheme.forShoppingCart(shopping_id), false, RegisterVersioned.class);

        ShoppingCart shop_c = shoppingCart.getValue();
        int qty = 0;
        float cost = 0;

        if (shop_c == null) {
            handler.commitAsync(commitListener);
            return;
        }

        Collection<SCLine> scLines = shop_c.getSCLines(handler);
        for (SCLine line : scLines) {
            int qty_read = line.getSCL_QTY(handler);
            qty += qty_read;
            cost += line.getSCL_COST();

        }

        Float SC_SUB_TOTAL = cost * (1 - 0.2f);
        Float SC_TAX = SC_SUB_TOTAL * 0.0825f;
        Float SC_SHIP_COST = 3.00f + (1.00f * qty);
        Float SC_TOTAL = SC_SUB_TOTAL + SC_SHIP_COST + SC_TAX;

        // Compute values but do not update them

        // shop_c.setSC_SUB_TOTAL(SC_SUB_TOTAL);
        // shop_c.setSC_TAX(SC_TAX);
        // shop_c.setSC_SHIP_COST(SC_SHIP_COST);
        // shop_c.setSC_TOTAL(SC_TOTAL);

        shoppingCart.set(shop_c);
        handler.commitAsync(commitListener);
    }

    /**
     * Confirmation of the purchase of a cart
     * 
     * @param customer
     *            the costumer id
     * @param cart
     *            the cart id
     * @throws NetworkException
     */
    public void BuyConfirm(String customer, String cart) throws NetworkException {

        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
        RegisterTxnLocal<ShoppingCart> obj;
        try {
            obj = (RegisterTxnLocal<ShoppingCart>) handler.get(TPCWNamingScheme.forShoppingCart(cart), false,
                    RegisterVersioned.class);

            RegisterTxnLocal<Customer> customerCRDT = (RegisterTxnLocal<Customer>) handler.get(
                    TPCWNamingScheme.forCustomer(customer), false, RegisterVersioned.class);

            ShoppingCart shop_c = obj.getValue();
            Customer customerObj = customerCRDT.getValue();

            double c_discount = (Double) customerObj.getC_DISCOUNT();

            String ship_addr_id = "";
            String cust_addr = (String) customerObj.getAddress();

            float decision = random.nextFloat();
            if (decision < 0.2) {
                ship_addr_id = insertAdress(handler);
            } else {
                ship_addr_id = cust_addr;
            }

            String[] ids = cart.split("\\.");
            int thread_id = Integer.parseInt(ids[1]);

            String shipping = ship_types[random.nextInt(ship_types.length)];

            float total = 0;
            try {
                total = (Float) shop_c.getSC_TOTAL(handler);
            } catch (Exception e) {
                e.printStackTrace();
            }

            String order_id = enterOrder(customer, thread_id, shop_c, ship_addr_id, cust_addr, shipping, total,
                    c_discount, handler);

            String cc_type = BenchmarkUtil.getRandomAString(10);
            long cc_number = BenchmarkUtil.getRandomNString(16);
            String cc_name = BenchmarkUtil.getRandomAString(30);
            long cc_expiry = System.currentTimeMillis() + random.nextInt(644444400);

            enterCCXact(order_id, customer, cc_type, cc_number, cc_name, cc_expiry, total, ship_addr_id.toString(),
                    handler);

        } catch (WrongTypeException e1) {
            e1.printStackTrace();
        } catch (NoSuchObjectException e1) {
            e1.printStackTrace();
        } catch (VersionNotFoundException e1) {
            e1.printStackTrace();
        }

        handler.commit();

    }

    /*
     * TODO: Assumes an order is palced only once. Ok for the benchmark, not ok
     * for the real world. What about if more items where added to the cart
     * while entering the order?
     */
    public String enterOrder(String customer_id, int thread_id, ShoppingCart shoppingCart, String ship_addr_id,
            String cust_addr, String shipping, float total, double c_discount, TxnHandle handler)
            throws NetworkException {

        try {
            String key = (String) keyGenerator.getNextKey(thread_id);
            float subTotal = (Float) shoppingCart.getSC_SUB_TOTAL(handler);
            float tax = subTotal * 0.025f;
            float ship = (Float) shoppingCart.getSC_SHIP_COST(handler);
            String shipType = ship_types[random.nextInt(ship_types.length)];
            String status = status_types[random.nextInt(status_types.length)];

            Order order = new Order(key, customer_id, System.currentTimeMillis(), subTotal, tax, subTotal + ship + tax,
                    shipType, System.currentTimeMillis() + random.nextInt(604800000), status, cust_addr, ship_addr_id);
            RegisterTxnLocal<Order> orderCRDT;
            orderCRDT = (RegisterTxnLocal<Order>) handler.get(TPCWNamingScheme.forOrder(key), true,
                    RegisterVersioned.class);

            orderCRDT.set(order);
            int index = 0;
            Collection<SCLine> items = shoppingCart.getSCLines(handler);

            for (SCLine item : items) {
                int qty = item.getSCL_QTY(handler);
                int item_id = item.getI_ID();

                String ol_key = key + "." + index;

                OrderLine orderLine = new OrderLine(ol_key, key, item_id, qty, c_discount,
                        BenchmarkUtil.getRandomAString(20, 100));

                index++;

                order.addOrderLine(orderLine, handler);

                int item_i = orderLine.OL_QTY;
                RegisterTxnLocal<Item> itemCRDT = (RegisterTxnLocal<Item>) handler.get(
                        TPCWNamingScheme.forItem(item_id + ""), false, RegisterVersioned.class);
                Item itemFromBucket = itemCRDT.getValue();

                if (itemFromBucket.getI_STOCK(handler) - item_i < 10)
                    itemFromBucket.addI_STOCK(21 - item_i, handler);
                else
                    itemFromBucket.addI_STOCK(-item_i, handler);

                itemFromBucket.addI_TOTAL_SOLD(item_i, handler);

                updateBestSeller(itemFromBucket, handler);
                updateOrdersIndex(order, handler);

            }

            RegisterTxnLocal<Customer> customerCRDT = (RegisterTxnLocal<Customer>) handler.get(
                    TPCWNamingScheme.forCustomer(customer_id), false, RegisterVersioned.class);
            Customer customer = customerCRDT.getValue();
            customer.setC_O_LAST_ID(key);

            return customer_id;

        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

        return customer_id;
    }

    private void updateOrdersIndex(Order order, TxnHandle handler) throws NetworkException {
        SetTxnLocalIndexByDate orderedOrders;
        try {
            orderedOrders = handler.get(TPCWNamingScheme.forOrdersIndex(), true, SetIndexByDate.class);
            orderedOrders.insert(order.getInfo());
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Updates the best seller items
     * 
     * @param itemFromBucket
     * @param handler
     * @throws NetworkException
     */
    private void updateBestSeller(Item itemFromBucket, TxnHandle handler) throws NetworkException {
        SetTxnLocalBestSellers bestSellers;
        try {
            bestSellers = handler.get(TPCWNamingScheme.forBestSellers(itemFromBucket.getI_SUBJECT()), true,
                    SetBestSellers.class);
            BestSellerEntry newbs = new BestSellerEntry(itemFromBucket.getI_SUBJECT(), itemFromBucket.getI_ID(),
                    itemFromBucket.getI_TOTAL_SOLD(handler));
            bestSellers.insert(newbs);
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void enterCCXact(String order_id, String customer, String cc_type, long cc_number, String cc_name,
            long cc_expiry, float total, String ship_addr_id, TxnHandle handler) throws NetworkException {

        RegisterTxnLocal<Address> address;
        try {
            address = (RegisterTxnLocal<Address>) handler.get(TPCWNamingScheme.forAddress(ship_addr_id), false,
                    RegisterVersioned.class);

            int co_id = 0;
            if (address.getValue() != null)
                co_id = address.getValue().getCountry_id();

            CCXactItem ccxactItem = new CCXactItem(cc_type, cc_number, cc_name, cc_expiry, total, cc_expiry, order_id,
                    co_id);

            RegisterTxnLocal<CCXactItem> ccxactitem = (RegisterTxnLocal<CCXactItem>) handler.get(
                    TPCWNamingScheme.forCCXactItem(order_id), true, RegisterVersioned.class);
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void OrderInquiry(String customer) throws NetworkException {

        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);
        RegisterTxnLocal<Customer> obj;
        try {
            obj = (RegisterTxnLocal<Customer>) handler.get(TPCWNamingScheme.forCustomer(customer), false,
                    RegisterVersioned.class);

            Customer customerObj = obj.getValue();

            if (customerObj.getC_O_LAST_ID() == null) {
                handler.commitAsync(commitListener);
                return;
            }

            RegisterTxnLocal<Order> orderCRDT = (RegisterTxnLocal<Order>) handler.get(
                    TPCWNamingScheme.forOrder(customerObj.getC_O_LAST_ID()), false, RegisterVersioned.class);
            Order orderObj = orderCRDT.getValue();
            Collection<OrderLine> orderLines = orderObj.getOrderLines(handler);
//            if (BULK_GET) {
//                Set<CRDTIdentifier> ids = new HashSet<CRDTIdentifier>();
//                ids.add(TPCWNamingScheme.forOrder(customerObj.getC_O_LAST_ID()));
//                ids.add(TPCWNamingScheme.forCCXactItem(customer));
//
//                for (OrderLine ol : orderLines)
//                    ids.add(TPCWNamingScheme.forItem(ol.getOL_I_ID() + ""));
//
//                handler.bulkGet(ids, false, TIMEOUT_BULK_OP, null);
//            }
            for (OrderLine ol : orderLines) {
                handler.get(TPCWNamingScheme.forItem(ol.getOL_I_ID() + ""), false, RegisterVersioned.class);
            }

            RegisterTxnLocal<CCXactItem> ccxactsCRDT = (RegisterTxnLocal<CCXactItem>) handler.get(
                    TPCWNamingScheme.forCCXactItem(customer), false, RegisterVersioned.class);

            CCXactItem ccxactsObj = ccxactsCRDT.getValue();

            RegisterTxnLocal<Address> o_bill_addrCRDT = (RegisterTxnLocal<Address>) handler.get(
                    TPCWNamingScheme.forAddress(orderObj.getO_BILL_ADDR_ID()), false, RegisterVersioned.class);

            RegisterTxnLocal<Address> o_ship_addrCRDT = (RegisterTxnLocal<Address>) handler.get(
                    TPCWNamingScheme.forAddress(orderObj.getO_BILL_ADDR_ID()), false, RegisterVersioned.class);

            RegisterTxnLocal<Country> o_ship_addr_co = (RegisterTxnLocal<Country>) handler.get(
                    TPCWNamingScheme.forCountry(o_ship_addrCRDT.getValue().getCountry_id() + ""), false,
                    RegisterVersioned.class);

            RegisterTxnLocal<Country> o_bill_addr_co = (RegisterTxnLocal<Country>) handler.get(
                    TPCWNamingScheme.forCountry(o_bill_addrCRDT.getValue().getCountry_id() + ""), false,
                    RegisterVersioned.class);

        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

        handler.commitAsync(commitListener);

    }

    public void insertItem(String itemId, Item item, int stock) {
        try {
            TxnHandle txh = threadTransaction.get();
            RegisterTxnLocal<Item> newItem = (RegisterTxnLocal<Item>) txh.get(TPCWNamingScheme.forItem(itemId), true,
                    RegisterVersioned.class);

            SetTxnLocalString indexesCRDT = txh.get(TPCWNamingScheme.forIndex(TPCWNamingScheme.getItemsTableName()),
                    true, SetStrings.class);

            indexesCRDT.insert(itemId);

            updateIndex(item.getI_SUBJECT(), item.getI_PUB_DATE().getTime(), item.getI_ID(), item.getI_TITLE(),
                    item.getI_AUTHOR(), txh);
            item.addI_STOCK(stock, txh);

            newItem.set(item);
        } catch (SwiftException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Insert order method for populator. Adds the order to the buffer and
     * updates the client's last order
     * 
     * @param order
     * @param orderLines
     * @param ccXact
     * @throws NetworkException
     */
    public void insertOrder(Order order, List<OrderLine> orderLines, CCXactItem ccXact) throws NetworkException {

        TxnHandle handler = threadTransaction.get();

        try {
            for (OrderLine ol : orderLines) {
                order.addOrderLine(ol, handler);
            }

            RegisterTxnLocal<Order> orderCRDT = (RegisterTxnLocal<Order>) handler.get(
                    TPCWNamingScheme.forOrder(order.getO_ID()), true, RegisterVersioned.class);

            for (OrderLine orderLine : orderLines) {
                RegisterTxnLocal<Item> itemCRDT = (RegisterTxnLocal<Item>) handler.get(
                        TPCWNamingScheme.forItem(orderLine.getOL_I_ID() + ""), false, RegisterVersioned.class);
                Item item = itemCRDT.getValue();

                // int sold = item.getI_TOTAL_SOLD(handler);
                // sold += orderLine.OL_QTY;
                item.addI_TOTAL_SOLD(orderLine.OL_QTY, handler);
                updateBestSeller(item, handler);
                updateOrdersIndex(order, handler);

            }

            orderCRDT.set(order);

            SetTxnLocalIndexByDate indexesCRDT = handler.get(TPCWNamingScheme.forOrdersIndex(), true,
                    SetIndexByDate.class);

            indexesCRDT.insert(order.getInfo());

            RegisterTxnLocal<Customer> costumerCRDT = (RegisterTxnLocal<Customer>) handler.get(
                    TPCWNamingScheme.forCustomer(order.getO_C_ID()), false, RegisterVersioned.class);
            Customer customer = costumerCRDT.getValue();
            customer.setC_O_LAST_ID(order.getO_ID());
            costumerCRDT.set(customer);

            RegisterTxnLocal<CCXactItem> ccXactCRDT = (RegisterTxnLocal<CCXactItem>) handler.get(
                    TPCWNamingScheme.forCCXactItem(order.getO_C_ID()), true, RegisterVersioned.class);

            ccXactCRDT.set(ccXact);

        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void buildSearchIndex(Collection<Map<String, Object>> collection) throws NetworkException {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);

//        if (BULK_GET) {
//            HashSet<CRDTIdentifier> ids = new HashSet<CRDTIdentifier>();
//            for (Map<String, Object> item : collection) {
//                ids.add(TPCWNamingScheme.forAuthor(item.get("I_A_ID") + ""));
//            }
//            handler.bulkGet(ids, false, TIMEOUT_BULK_OP, null);
//        }
        for (Map<String, Object> item : collection) {
            titleSearchIndex.insert((String) item.get("I_TITLE"), item.get("I_ID") + "");
            int authorId = (Integer) item.get("I_A_ID");
            RegisterTxnLocal<Author> authorReg;
            try {
                authorReg = (RegisterTxnLocal<Author>) handler.get(TPCWNamingScheme.forAuthor(authorId + ""), false,
                        RegisterVersioned.class);

                Author author = authorReg.getValue();
                List<String> authorItems = authorFirstNameSearchIndex.find(author.getA_FNAME());
                if (authorItems == null) {
                    authorItems = new LinkedList<String>();
                    authorFirstNameSearchIndex.insert(author.getA_FNAME(), authorItems);
                }
                authorItems.add(item.get("I_ID") + "");

                authorItems = authorMiddleNameSearchIndex.find(author.getA_MNAME());
                if (authorItems == null) {
                    authorItems = new LinkedList<String>();
                    authorMiddleNameSearchIndex.insert(author.getA_MNAME(), authorItems);
                }
                authorItems.add(item.get("I_ID") + "");

                authorItems = authorLastNameSearchIndex.find(author.getA_LNAME());
                if (authorItems == null) {
                    authorItems = new LinkedList<String>();
                    authorLastNameSearchIndex.insert(author.getA_LNAME(), authorItems);
                }
                authorItems.add(item.get("I_ID") + "");

                List<String> itemsPerSubject = subjectSearchIndex.get(item.get("I_SUBJECT"));
                if (itemsPerSubject == null) {
                    itemsPerSubject = new LinkedList<String>();
                    subjectSearchIndex.put((String) item.get("I_SUBJECT"), itemsPerSubject);
                }
                itemsPerSubject.add(item.get("I_ID") + "");
            } catch (WrongTypeException e) {
                e.printStackTrace();
            } catch (NoSuchObjectException e) {
                e.printStackTrace();
            } catch (VersionNotFoundException e) {
                e.printStackTrace();
            }
        }

        handler.commitAsync(commitListener);
    }

    public void doSearch(String searchAttribute, String value) throws NetworkException {
        if (searchAttribute.equalsIgnoreCase("SUBJECT")) {
            List<String> itemsOfSubject = subjectSearchIndex.get(value);
            int count = 0;
            if (itemsOfSubject != null && itemsOfSubject.size() > 0) {
                TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);
//                if (BULK_GET) {
//                    HashSet<CRDTIdentifier> ids = new HashSet<CRDTIdentifier>();
//                    for (int i = 0; i < 50 && i < itemsOfSubject.size(); i++) {
//                        ids.add(TPCWNamingScheme.forItem(itemsOfSubject.get(i)));
//                    }
//                    handler.bulkGet(ids, false, TIMEOUT_BULK_OP, null);
//                }
                for (String item : itemsOfSubject) {
                    if (count == 50)
                        break;
                    try {
                        handler.get(TPCWNamingScheme.forItem(item), false, RegisterVersioned.class);
                        count++;
                    } catch (WrongTypeException e) {
                        e.printStackTrace();
                    } catch (NoSuchObjectException e) {
                        e.printStackTrace();
                    } catch (VersionNotFoundException e) {
                        e.printStackTrace();
                    }

                }
                handler.commitAsync(commitListener);
            }

        } else if (searchAttribute.equalsIgnoreCase("AUTHOR")) {
            List<List<String>> itemsPerAuthor = authorFirstNameSearchIndex.searchPrefix(value, RECORD_SEARCH_LIMIT);
            itemsPerAuthor.addAll(authorMiddleNameSearchIndex.searchPrefix(value, RECORD_SEARCH_LIMIT));
            itemsPerAuthor.addAll(authorLastNameSearchIndex.searchPrefix(value, RECORD_SEARCH_LIMIT));
            int count = 0;
            if (itemsPerAuthor.size() > 0) {
                TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);

//                if (BULK_GET) {
//                    HashSet<CRDTIdentifier> ids = new HashSet<CRDTIdentifier>();
//                    for (List<String> authorItems : itemsPerAuthor) {
//                        if (count == 50)
//                            break;
//                        for (String item : authorItems) {
//                            if (count == 50)
//                                break;
//                            ids.add(TPCWNamingScheme.forItem(item));
//                            count++;
//                        }
//                    }
//                    handler.bulkGet(ids, false, TIMEOUT_BULK_OP, null);
//                }
                for (List<String> authorItems : itemsPerAuthor) {
                    if (count == 50)
                        break;
                    for (String item : authorItems) {
                        if (count == 50)
                            break;
                        try {
                            handler.get(TPCWNamingScheme.forItem(item), false, RegisterVersioned.class);
                            count++;
                        } catch (WrongTypeException e) {
                            e.printStackTrace();
                        } catch (NoSuchObjectException e) {
                            e.printStackTrace();
                        } catch (VersionNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                }
                handler.commitAsync(commitListener);
            }

        } else if (searchAttribute.equalsIgnoreCase("TITLE")) {
            List<String> itemsWithTitle = titleSearchIndex.searchPrefix(value, RECORD_SEARCH_LIMIT);
            int count = 0;
            if (itemsWithTitle.size() > 0) {
                TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);
//                if (BULK_GET) {
//                    HashSet<CRDTIdentifier> ids = new HashSet<CRDTIdentifier>();
//                    for (int i = 0; i < 50 && i < itemsWithTitle.size(); i++) {
//                        ids.add(TPCWNamingScheme.forItem(itemsWithTitle.get(i)));
//                    }
//                    handler.bulkGet(ids, false, TIMEOUT_BULK_OP, null);
//                }
                for (String item : itemsWithTitle) {
                    if (count == 50)
                        break;
                    try {
                        Item itemObj = (Item) handler.get(TPCWNamingScheme.forItem(item), false,
                                RegisterVersioned.class).getValue();
                        count++;
                    } catch (WrongTypeException e) {
                        e.printStackTrace();
                    } catch (NoSuchObjectException e) {
                        e.printStackTrace();
                    } catch (VersionNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                handler.commitAsync(commitListener);
            }

        } else {
            System.out.println("OPTION NOT RECOGNIZED");
        }
    }

    // TODO: Items should be ordered;
    public void newProducts(String field) throws NetworkException {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);
        try {
            SetTxnLocalAuthorIndex newItems = handler.get(TPCWNamingScheme.forIndex(field), true, SetAuthorIndex.class);
//            if (BULK_GET) {
//                HashSet<CRDTIdentifier> ids = new HashSet<CRDTIdentifier>();
//                for (AuthorIndex key : newItems.getOrderedValues()) {
//                    ids.add(TPCWNamingScheme.forItem(key.getI_ID()));
//                }
//                handler.bulkGet(ids, false, TIMEOUT_BULK_OP, null);
//            }
            for (AuthorIndex key : newItems.getOrderedValues()) {
                RegisterTxnLocal<Item> item = (RegisterTxnLocal<Item>) handler.get(
                        TPCWNamingScheme.forItem(key.getI_ID()), false, RegisterVersioned.class);
                item.getValue();
            }
        }

        catch (swift.exceptions.NoSuchObjectException e) {

        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

        handler.commitAsync(commitListener);
    }

    public Map<String, Map<String, Map<String, Object>>> getResults() throws Exception {
        // not called and not implemented
        return null;
    }

    public void BestSellers(String subject) throws NetworkException {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);
        try {
            SetTxnLocalBestSellers bestSellersSubject = handler.get(TPCWNamingScheme.forBestSellers(subject), true,
                    SetBestSellers.class);

            Set<BestSellerEntry> bestSellers = bestSellersSubject.getValue();
            PriorityQueue<BestSellerEntry> orderedBestSellers = new PriorityQueue<BestSellerEntry>(50,
                    new Comparator<BestSellerEntry>() {

                        @Override
                        public int compare(BestSellerEntry o1, BestSellerEntry o2) {
                            return o1.getI_TOTAL_SOLD() - o2.getI_TOTAL_SOLD();
                        }
                    });
            orderedBestSellers.addAll(bestSellers);
        } catch (swift.exceptions.NoSuchObjectException e) {
            e.printStackTrace();
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }
        handler.commitAsync(commitListener);
    }

    /**
     * Gets item info
     * 
     * @param id
     *            the id of the item
     * @throws NetworkException
     */
    public void ItemInfo(int id) throws NetworkException {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, true);
        RegisterTxnLocal<Item> obj;
        try {
            obj = (RegisterTxnLocal<Item>) handler.get(TPCWNamingScheme.forItem(id + ""), false,
                    RegisterVersioned.class/*
                                            * , updateListener
                                            */);
            Item item = obj.getValue();
            RegisterTxnLocal<Author> authorObj = (RegisterTxnLocal<Author>) handler.get(
                    TPCWNamingScheme.forAuthor(item.getI_AUTHOR() + ""), false, RegisterVersioned.class);
            Author author = authorObj.getValue();

        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

        handler.commitAsync(commitListener);

    }

    /*
     * Get an item and add it to the subject index. Gets five best seller items
     * among 10K and update item's related
     */
    public void AdminChange(int item_id) throws NetworkException {
        System.out.println("ADMIN CHANGE");
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
        RegisterTxnLocal<Item> obj;
        try {
            obj = (RegisterTxnLocal<Item>) handler.get(TPCWNamingScheme.forItem(item_id + ""), false,
                    RegisterVersioned.class);
            Item item = obj.getValue();
            long date = item.getI_PUB_DATE().getTime();

            String subject = (String) item.getI_SUBJECT();
            int author_id = (Integer) item.getI_AUTHOR();
            String title = (String) item.getI_TITLE();

            updateIndex(subject, date, item_id, title, author_id, handler);

            Map<String, Entity> orders = getRecentOrders(NUM_ORDERS_ADMIN, TPCWNamingScheme.forOrdersIndex(), handler);

            Map<Integer, Integer> items_info = new TreeMap<Integer, Integer>();

            for (Entity orders_info : orders.values()) {
                boolean found = false;
                Collection<OrderLine> orderLines = ((Order) orders_info).getOrderLines(handler);
                TreeMap<Integer, Integer> bought_items = new TreeMap<Integer, Integer>();
                for (OrderLine order_line : orderLines) {
                    int i_id = (Integer) order_line.getOL_I_ID();
                    if (i_id == item_id) {
                        found = true;

                    } else {
                        int item_qty = (Integer) order_line.getOL_QTY();
                        bought_items.put(i_id, item_qty);
                    }
                }

                if (found == true) {
                    for (Integer i_id : bought_items.keySet()) {
                        if (items_info.containsKey(i_id)) {
                            int current_qty = items_info.get(i_id);
                            items_info.put(i_id, (bought_items.get(i_id) + current_qty));
                        } else {
                            items_info.put(i_id, bought_items.get(i_id));
                        }
                    }
                }
            }

            Map top_sellers = reverseSortByValue(items_info);

            List<Integer> best = new ArrayList<Integer>();
            int num = 0;
            for (Iterator<Integer> it = top_sellers.keySet().iterator(); it.hasNext();) {
                int key = it.next();
                best.add(key);
                num++;
                if (num == 5)
                    break;
            }

            if (num < 5) {
                for (int i = num; i < 5; i++) {
                    best.add(random.nextInt(Constants.NUM_ITEMS));
                }

            }

            item.setI_PUB_DATE(System.currentTimeMillis());
            item.setI_IMAGE(new String("img" + random.nextInt(1000) % 100 + "/image_" + random.nextInt(1000) + ".gif"));
            item.setI_THUMBNAIL(new String("img" + random.nextInt(1000) % 100 + "/thumb" + random.nextInt(1000)
                    + ".gif"));
            item.setI_RELATED1(best.get(0));
            item.setI_RELATED2(best.get(1));
            item.setI_RELATED3(best.get(2));
            item.setI_RELATED4(best.get(3));
            item.setI_RELATED5(best.get(4));
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

        handler.commitAsync(commitListener);
    }

    private Entity insertOrModifyAttribute(Entity entity, Object value, String column) throws NetworkException,
            NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Class<? extends Entity> cls = entity.getClass();
        Field fld = cls.getField(column);
        fld.set(entity, value);
        return entity;
    }

    private Class<?> getEntityClass(String string) {
        String className = string.substring(0, 1).toUpperCase() + string.substring(1);
        if (className.equals("Item"))
            return Item.class;
        try {
            return Class.forName("pt.citi.cs.crdt.benchmarks.TPCW_SwiftCloud.entities." + className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void updateIndex(String subject, long date, int item_id, String title, int author_key, TxnHandle handler)
            throws NetworkException {

        try {
            RegisterTxnLocal<Author> authorCRDT;
            authorCRDT = (RegisterTxnLocal<Author>) handler.get(TPCWNamingScheme.forAuthor(author_key + ""), false,
                    RegisterVersioned.class);
            Author author = authorCRDT.getValue();

            AuthorIndex authorIndex = new AuthorIndex(author.getA_FNAME(), author.getA_LNAME(), title, date, item_id
                    + "");

            Date d = new Date(System.currentTimeMillis());
            Long new_time_stamp = Long.MAX_VALUE - d.getTime();
            String new_index_key = new_time_stamp + "." + item_id;

            SetTxnLocalAuthorIndex authorIndexCRDT = handler.get(TPCWNamingScheme.forIndex(subject), true,
                    SetAuthorIndex.class);

            authorIndexCRDT.insert(authorIndex);
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }

    }

    static Map reverseSortByValue(Map map) {
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
            }
        });
        Collections.reverse(list);
        Map result = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            result.put(entry.getKey(), entry.getValue());
        }

        return result;

    }

    public void startTransactionForThread() throws NetworkException {
        TxnHandle handler = localSession.beginTxn(ISOLATION_LEVEL, CACHE_POLICY, false);
        threadTransaction.set(handler);
    }

    public void endTransactionForThread() throws NetworkException {
        TxnHandle ongoing = threadTransaction.get();
        ongoing.commit();
        threadTransaction.remove();
    }

    public void disposeClient() {
        try {
            if (bufferedOutput != null) {
                bufferedOutput.flush();
                bufferedOutput.close();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        localSession.stopScout(true);
    }

    public void setOutput(String outputFileName) throws IOException {
        FileWriter output = new FileWriter(outputFileName);
        bufferedOutput = new BufferedWriter(output);
        bufferedOutput.write("START_TIME\t" + System.currentTimeMillis() + "\n");
        bufferedOutput.write("THREAD_ID\tOPERATION\tSTART_TIME\tOP_RECEIVED_TIME\tOP_EXECUTION_END_TIME\tTOTAL_TIME"
                + "\n");
    }

}

class TPCWCommitListener implements CommitListener {

    @Override
    public void onGlobalCommit(TxnHandle transaction) {

    }

}

class TPCWUpdateListener implements ObjectUpdatesListener {

    @Override
    public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {

    }

    @Override
    public boolean isSubscriptionOnly() {
        return false;
    }

}

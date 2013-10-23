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
package pt.citi.cs.crdt.benchmarks.tpcw.populator;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.generic.Constants;
import org.uminho.gsd.benchmarks.generic.helpers.NodeKeyGenerator;
import org.uminho.gsd.benchmarks.helpers.BenchmarkUtil;
import org.uminho.gsd.benchmarks.interfaces.Entity;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;
import org.uminho.gsd.benchmarks.interfaces.populator.AbstractBenchmarkPopulator;

import pt.citi.cs.crdt.benchmarks.tpcw.database.TPCW_SwiftCloud_Executor;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.Address;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.Author;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.CCXactItem;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.Country;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.Customer;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.TPCWNamingScheme;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.Item;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.Order;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt.OrderLine;
import swift.exceptions.NetworkException;

//There is a configuration file to specify the number of thread the populator executes
public class SwiftCloudPopulator extends AbstractBenchmarkPopulator {

    public static final int TX_SIZE = 50;
    /**
     * Time measurements
     */
    private static boolean delay_inserts = false;
    private static int delay_time = 100;
    private static Random rand = new Random();
    private int rounds = 500;
    private ResultHandler results;
    String result_path;

    // ATTENTION: The NUM_EBS and NUM_ITEMS variables are the only variables
    // that should be modified in order to rescale the DB.
    private static/* final */int NUM_EBS = Constants.NUM_EBS;
    private static/* final */int NUM_ITEMS = Constants.NUM_ITEMS;
    private static/* final */int NUM_CUSTOMERS = Constants.NUM_CUSTOMERS;
    private static/* final */int NUM_ADDRESSES = Constants.NUM_ADDRESSES;
    private static/* final */int NUM_AUTHORS = Constants.NUM_AUTHORS;
    private static/* final */int NUM_ORDERS = Constants.NUM_ORDERS;
    private static/* final */int NUM_COUNTRIES = Constants.NUM_COUNTRIES; // this
                                                                          // is
                                                                          // constant.
                                                                          // Never
                                                                          // changes!

    private static AbstractDatabaseExecutorFactory databaseClientFactory;
    ArrayList<Author> authors = new ArrayList<Author>();

    ArrayList<String> addresses = new ArrayList<String>();
    ArrayList<Integer> countries = new ArrayList<Integer>();
    ArrayList<String> customers = new ArrayList<String>();
    ArrayList<Integer> items = new ArrayList<Integer>();

    boolean build_indexes = true;

    boolean debug = true;
    private static int num_threads = 5;
    boolean error = false;
    private CountDownLatch barrier;

    private static boolean client_error = false;

    public SwiftCloudPopulator(AbstractDatabaseExecutorFactory database_interface_factory, String conf_filename) {
        super(database_interface_factory, conf_filename);

        // Sys.init();
        // startSequencer("localhost");
        // startDCServer("localhost");

        databaseClientFactory = database_interface_factory;

        Map<String, String> execution_info = configuration.get("BenchmarkPopulator");

        String name = execution_info.get("name");
        if (name == null || name.isEmpty()) {
            name = "TPCW_POPULATOR";
            System.out.println("[WARN:] NO DEFINED NAME: DEFAULT -> TPCW_POPULATOR ");
        }

        String num_threads_info = execution_info.get("thread_number");
        if (num_threads_info == null || num_threads_info.isEmpty()) {
            num_threads = 1;
            System.out.println("[WARN:] NO THREAD NUMBER DEFINED: DEFAULT -> 1");
        } else {
            num_threads = Integer.parseInt(num_threads_info.trim());
        }

        String do_delays = execution_info.get("delay_inserts");
        if (do_delays == null || do_delays.isEmpty()) {
            delay_inserts = false;
            System.out.println("[WARN:] NO DELAY OPTION DEFINED, DELAYS NOT USED");
        } else {
            delay_inserts = Boolean.valueOf(do_delays.trim());
        }

        if (delay_inserts) {
            String delay_time_info = execution_info.get("delay_time");
            if (delay_time_info == null || delay_time_info.isEmpty()) {
                delay_time = 10;
                System.out.println("[WARN:] NO DELAY TIME DEFINED, DEFAULT: 10ms");
            } else {
                delay_time = Integer.valueOf(delay_time_info.trim());
            }
        }

        this.results = new ResultHandler(name, -1);

        result_path = execution_info.get("result_path");
        if (result_path == null || result_path.trim().isEmpty()) {
            result_path = "./results";
        }

        String ebs = execution_info.get("tpcw_numEBS");
        if (ebs != null) {
            Constants.NUM_EBS = Integer.valueOf(ebs.trim());
        } else {
            System.out.println("SCALE FACTOR (EBS) NOT DEFINED. SET TO: " + NUM_EBS);
        }

        String items = execution_info.get("tpcw_numItems");

        if (items != null) {
            Constants.NUM_ITEMS = Integer.valueOf(items.trim());
        } else {
            System.out.println("NUMBER OF ITEMS NOT DEFINED. SET TO: " + NUM_ITEMS);
        }

        Constants.NUM_CUSTOMERS = Constants.NUM_EBS * 2880;
        Constants.NUM_ADDRESSES = 2 * Constants.NUM_CUSTOMERS;
        Constants.NUM_AUTHORS = (int) (.25 * Constants.NUM_ITEMS);
        Constants.NUM_ORDERS = (int) (.9 * Constants.NUM_CUSTOMERS);

        NUM_EBS = Constants.NUM_EBS;
        NUM_ITEMS = Constants.NUM_ITEMS;
        NUM_CUSTOMERS = Constants.NUM_CUSTOMERS;
        NUM_ADDRESSES = Constants.NUM_ADDRESSES;
        NUM_AUTHORS = Constants.NUM_AUTHORS;
        NUM_ORDERS = Constants.NUM_ORDERS;

    }

    public boolean populate() {
        delay_time = 1000;

        long startTime = System.currentTimeMillis();

        if (error) {
            return false;
        } else {

            // try {
            // removeALL();
            // } catch (Exception e) {
            // e.printStackTrace();
            // }

            try {

                insertCountries(NUM_COUNTRIES);
                if (client_error)
                    return false;
                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertAddresses(NUM_ADDRESSES, true);
                if (client_error)
                    return false;

                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertCustomers(NUM_CUSTOMERS);
                if (client_error)
                    return false;

                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertAuthors(NUM_AUTHORS, true);
                if (client_error)
                    return false;

                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertItems(NUM_ITEMS);
                if (client_error)
                    return false;

                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertOrder_and_CC_XACTS(NUM_ORDERS);
                if (client_error)
                    return false;

                System.out.println("***Finished***" + (System.currentTimeMillis() - startTime) / 1000);

                long cool_down = 6000;
                System.out.println("Cooling down for " + (cool_down / 1000) + "s");
                ((TPCW_SwiftCloud_Executor) databaseClientFactory.getDatabaseClient()).closeScout();
                Thread.sleep(cool_down);

            } catch (InterruptedException ex) {
                Logger.getLogger(SwiftCloudPopulator.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            } catch (Exception ex) {
                Logger.getLogger(SwiftCloudPopulator.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            }

            
            results.listDataToSOutput();
            results.listDatatoFiles(result_path, "", true);
            results.cleanResults();
            return true;
        }
    }

    public void cleanDB() throws Exception {
        /* removeALL(); */
    }

    public void BenchmarkClean() throws Exception {
        // TODO: Implement only if needed
    }

    /*
     * public void removeALL() throws Exception {
     * 
     * final String[] buckets = { "country", "customer", "item_subject_index",
     * "item_title_index", "item_author_index", "item", "order", "order_Line",
     * "CC_XACTS", "Author", "Address", "shopping_cart", "Shopping_cart_line",
     * "Results" };
     * 
     * barrier = new CountDownLatch(7);
     * 
     * final DatabaseExecutorInterface client = databaseClientFactory
     * .getDatabaseClient();
     * 
     * System.out.println(">>Cleaning Database");
     * 
     * for (int i = 0; i < 7; i++) { final int j = i; Runnable r = new
     * Runnable() {
     * 
     * public void run() { try { client.truncate(buckets[j]);
     * System.out.println(">>Endend Cleaning " + buckets[j]);
     * barrier.countDown();
     * 
     * } catch (Exception e) { e.printStackTrace(); } } };
     * 
     * Thread t = new Thread(r); t.start(); }
     * 
     * barrier.await();
     * 
     * barrier = new CountDownLatch(buckets.length - 7);
     * 
     * final DatabaseExecutorInterface client2 = databaseClientFactory
     * .getDatabaseClient();
     * 
     * for (int i = 7; i < buckets.length; i++) { final int j = i; Runnable r =
     * new Runnable() {
     * 
     * public void run() { try { client2.truncate(buckets[j]);
     * System.out.println(">>Endend Cleaning " + buckets[j]);
     * barrier.countDown();
     * 
     * } catch (Exception e) { e.printStackTrace(); } } };
     * 
     * Thread t = new Thread(r); t.start(); }
     * 
     * barrier.await();
     * 
     * System.out.println("Database Cleaned"); client.closeClient(); }
     */
    public void databaseInsert(DatabaseExecutorInterface client, String Operation, String key, String path,
            Entity value, ResultHandler results) throws Exception {

        long time1 = System.currentTimeMillis();
        client.insert(key, path, value);
        long time2 = System.currentTimeMillis();
        results.logResult(Operation, time2 - time1);

    }

    /************************************************************************/
    /************************************************************************/
    /************************************************************************/

    /**
     * ************ Authors* **************
     */

    public void insertAuthors(int n, boolean insert) throws InterruptedException {
        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        System.out.println(">>Inserting " + n + " Authors || populatores " + num_threads);
        barrier = new CountDownLatch(threads);

        AuthorPopulator[] partial_authors = new AuthorPopulator[threads];
        for (int i = threads; i > 0; i--) {

            int base = (threads - i) * sections;

            AuthorPopulator populator = null;
            if (i == 0) {
                populator = new AuthorPopulator(firstSection, base, insert);

            } else {
                populator = new AuthorPopulator(sections, base, insert);
            }
            partial_authors[threads - i] = populator;
            Thread t = new Thread(populator);
            t.start();
        }

        barrier.await();
        for (AuthorPopulator populator : partial_authors) {
            ArrayList<Author> ids = populator.getData();
            for (Author id : ids) {
                authors.add(id);
            }
            if (insert)
                results.addResults(populator.returnResults());
            populator.partial_results.cleanResults();

        }
        partial_authors = null;
        System.gc();

    }

    class AuthorPopulator implements Runnable {

        int num_authors;
        DatabaseExecutorInterface client;
        ArrayList<Author> partial_authors;
        ResultHandler partial_results;
        boolean insertDB;
        int base;

        public AuthorPopulator(int num_authors, int base, boolean insertDB) {
            client = databaseClientFactory.getDatabaseClient();
            this.num_authors = num_authors;
            partial_authors = new ArrayList<Author>();
            partial_results = new ResultHandler("", rounds);
            this.insertDB = insertDB;
            this.base = base;
        }

        public void run() {
            this.insertAuthors(num_authors);
        }

        public void databaseInsert(String Operation, String key, String path, Entity value, ResultHandler results)
                throws Exception {

            long time1 = System.currentTimeMillis();
            client.insert(key, path, value);
            long time2 = System.currentTimeMillis();
            results.logResult(Operation, time2 - time1);
        }

        public void insertAuthors(int n) {

            System.out.println("Inserting Authors: " + n);
            int lastStart = -1;
            for (int i = 0; i < n; i++) {
                if (i % TX_SIZE == 0) {
                    lastStart = i;
                    try {
                        ((TPCW_SwiftCloud_Executor) client).startTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }
                }
                GregorianCalendar cal = BenchmarkUtil.getRandomDate(1800, 1990);

                String[] names = (BenchmarkUtil.getRandomAString(3, 20) + " " + BenchmarkUtil.getRandomAString(2, 20))
                        .split(" ");
                String[] Mnames = ("d " + BenchmarkUtil.getRandomAString(2, 20)).split(" ");

                String first_name = names[0];
                String last_name = names[1];
                String middle_name = Mnames[1];
                java.sql.Date dob = new java.sql.Date(cal.getTime().getTime());
                String bio = BenchmarkUtil.getRandomAString(125, 500);
                // String key = first_name + middle_name + last_name
                // + rand.nextInt(1000);

                Author a = new Author(base + i, first_name, last_name, middle_name, dob.toString(), bio);
                if (insertDB)
                    try {
                        databaseInsert("INSERT_Authors", (base + i) + "", TPCWNamingScheme.getAuthorTableName(), a,
                                partial_results);
                    } catch (Exception e) {
                        e.printStackTrace();
                        client_error = true;
                        break;
                    }

                partial_authors.add(a);

                if ((i + 1) - lastStart == TX_SIZE || i + 1 == n)
                    try {
                        ((TPCW_SwiftCloud_Executor) client).endTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }
            }
            if (debug) {
                System.out.println("Thread finished: " + num_authors + " authors inserted");
            }

            barrier.countDown();
            client.closeClient();

        }

        public ArrayList<Author> getData() {
            return partial_authors;
        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

    /**
     * ************ Customers* **************
     */
    public void insertCustomers(int n) throws InterruptedException {

        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }
        System.out.println(">>Inserting " + n + " Customers || populatores " + num_threads);
        barrier = new CountDownLatch(threads);

        CustomerPopulator[] partial_Customers = new CustomerPopulator[threads];
        for (int i = threads; i > 0; i--) {

            int base = (threads - i) * sections;
            CustomerPopulator populator = null;
            if (i == 0) {
                populator = new CustomerPopulator(firstSection, base);

            } else {
                populator = new CustomerPopulator(sections, base);
            }
            partial_Customers[threads - i] = populator;
            Thread t = new Thread(populator);
            t.start();
        }
        barrier.await();
        for (CustomerPopulator populator : partial_Customers) {
            ArrayList<String> ids = populator.getData();
            for (String id : ids) {
                customers.add(id);
            }
            results.addResults(populator.returnResults());
            populator.partial_results.cleanResults();

        }
        partial_Customers = null;
        System.gc();

    }

    class CustomerPopulator implements Runnable {

        DatabaseExecutorInterface client;
        int num_Customers;
        ArrayList<String> partial_Customers;
        ResultHandler partial_results;
        int base;

        public CustomerPopulator(int num_Customers, int base) {
            client = databaseClientFactory.getDatabaseClient();
            this.num_Customers = num_Customers;
            partial_Customers = new ArrayList<String>();
            partial_results = new ResultHandler("", rounds);
            this.base = base;

        }

        public void run() {
            this.insertCustomers(num_Customers);
        }

        public void insertCustomers(int n) {

            System.out.println("Inserting Customers: " + n);
            int lastStart = -1;
            for (int i = 0; i < n; i++) {

                if ((i % TX_SIZE) == 0) {
                    lastStart = i;
                    try {
                        ((TPCW_SwiftCloud_Executor) client).startTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }
                }
                String name = (BenchmarkUtil.getRandomAString(8, 13) + " " + BenchmarkUtil.getRandomAString(8, 15));
                String[] names = name.split(" ");
                Random r = new Random();
                int random_int = r.nextInt(1000);

                String key = names[0] + "_" + (base + i);
                if (base + i >= 1000) {
                    key = names[0] + "_" + random_int;
                }
                // if(key.length()>=20){
                // System.out.println("TTT");
                // }

                String pass = names[0].charAt(0) + names[1].charAt(0) + "" + random_int;
                // insert(pass, key, "Customer", "C_PASSWD", writeCon);

                String first_name = names[0];
                // insert(first_name, key, "Customer", "C_FNAME", writeCon);

                String last_name = names[1];
                // insert(last_name, key, "Customer", "C_LNAME", writeCon);

                int phone = r.nextInt(999999999 - 100000000) + 100000000;
                // insert(phone, key, "Customer", "C_PHONE", writeCon);

                String email = key + "@" + BenchmarkUtil.getRandomAString(2, 9) + ".com";
                // insert(email, key, "Customer", "C_EMAIL", writeCon);

                double discount = r.nextDouble();
                // insert(discount, key, "Customer", "C_DISCOUNT", writeCon);

                // String adress = "Street: "
                // + (BenchmarkUtil.getRandomAString(8, 15) + " " +
                // BenchmarkUtil
                // .getRandomAString(8, 15)) + " number: "
                // + r.nextInt(500);
                // insert(adress, key, "Customer", "C_PHONE", writeCon);

                double C_BALANCE = 0.00;
                // insert(C_BALANCE, key, "Customer", "C_BALANCE", writeCon);

                double C_YTD_PMT = (double) BenchmarkUtil.getRandomInt(0, 99999) / 100.0;
                // insert(C_YTD_PMT, key, "Customer", "C_YTD_PMT", writeCon);

                GregorianCalendar cal = new GregorianCalendar();
                cal.add(Calendar.DAY_OF_YEAR, -1 * BenchmarkUtil.getRandomInt(1, 730));

                java.sql.Date C_SINCE = new java.sql.Date(cal.getTime().getTime());
                // insert(C_SINCE, key, "Customer", "C_SINCE ", writeCon);

                cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(0, 60));
                if (cal.after(new GregorianCalendar())) {
                    cal = new GregorianCalendar();
                }

                java.sql.Date C_LAST_LOGIN = new java.sql.Date(cal.getTime().getTime());
                // insert(C_LAST_LOGIN, key, "Customer", "C_LAST_LOGIN",
                // writeCon);

                java.sql.Timestamp C_LOGIN = new java.sql.Timestamp(System.currentTimeMillis());
                // insert(C_LOGIN, key, "Customer", "C_LOGIN", writeCon);

                cal = new GregorianCalendar();
                cal.add(Calendar.HOUR, 2);

                java.sql.Timestamp C_EXPIRATION = new java.sql.Timestamp(cal.getTime().getTime());
                // insert(C_EXPIRATION, key, "Customer", "C_EXPIRATION",
                // writeCon);

                cal = BenchmarkUtil.getRandomDate(1880, 2000);
                java.sql.Date C_BIRTHDATE = new java.sql.Date(cal.getTime().getTime());
                // insert(C_BIRTHDATE, key, "Customer", "C_BIRTHDATE",
                // writeCon);

                String C_DATA = BenchmarkUtil.getRandomAString(100, 500);
                // insert(C_DATA, key, "Customer", "C_DATA", writeCon);

                String address_id = addresses.get(rand.nextInt(addresses.size()));
                // insert(address.getAddr_id(), key, "Customer", "C_ADDR_ID",
                // writeCon);

                Customer c = new Customer(base + i + "", key, pass, last_name, first_name, phone + "", email,
                        C_SINCE.toString(), C_LAST_LOGIN.toString(), C_LOGIN.toString(), C_EXPIRATION.toString(),
                        C_BALANCE, C_YTD_PMT, C_BIRTHDATE.toString(), C_DATA, discount, address_id);

                try {
                    databaseInsert("INSERT_Customers", (base + i) + "", TPCWNamingScheme.getCustomersTableName(), c,
                            partial_results);
                } catch (Exception e) {
                    e.printStackTrace();
                    client_error = true;
                    break;

                }

                partial_Customers.add(base + i + "");

                if ((i + 1) - lastStart == TX_SIZE || i + 1 == n)
                    try {
                        ((TPCW_SwiftCloud_Executor) client).endTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }

            }
            if (debug) {
                System.out.println("Thread finished: " + num_Customers + " Customers inserted");
            }

            barrier.countDown();
            client.closeClient();
        }

        public ArrayList<String> getData() {
            return partial_Customers;
        }

        public ResultHandler returnResults() {
            return partial_results;
        }

        public void databaseInsert(String Operation, String key, String path, Entity value, ResultHandler results)
                throws Exception {

            long time1 = System.currentTimeMillis();
            client.insert(key, path, value);
            long time2 = System.currentTimeMillis();
            results.logResult(Operation, time2 - time1);

        }

    }

    /**
     * ************ Items* **************
     */
    public void insertItems(int n) throws InterruptedException {
        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        System.out.println(">>Inserting " + n + " Items || populatores " + num_threads);
        barrier = new CountDownLatch(threads);

        ItemPopulator[] partial_items = new ItemPopulator[threads];
        for (int i = threads; i > 0; i--) {

            int base = (threads - i) * sections;

            ItemPopulator populator = null;
            if (i == 0) {
                populator = new ItemPopulator(firstSection, base);

            } else {
                populator = new ItemPopulator(sections, base);
            }
            partial_items[threads - i] = populator;
            Thread t = new Thread(populator);
            t.start();
        }
        barrier.await();

        for (ItemPopulator populator : partial_items) {
            ArrayList<Integer> ids = populator.getData();
            for (int id : ids) {
                items.add(id);
            }
            results.addResults(populator.returnResults());
            populator.partial_results.cleanResults();
        }
        partial_items = null;
        System.gc();

    }

    class ItemPopulator implements Runnable {

        DatabaseExecutorInterface client;
        int num_items;
        ArrayList<Integer> partial_items;
        ResultHandler partial_results;
        int base = 0;

        public ItemPopulator(int num_items, int base) {
            client = databaseClientFactory.getDatabaseClient();
            this.num_items = num_items;
            partial_items = new ArrayList<Integer>();
            partial_results = new ResultHandler("", rounds);
            this.base = base;
        }

        public void run() {
            this.insertItems(num_items);
        }

        public void insertItems(int n) {

            String[] subjects = { "ARTS", "BIOGRAPHIES", "BUSINESS", "CHILDREN", "COMPUTERS", "COOKING", "HEALTH",
                    "HISTORY", "HOME", "HUMOR", "LITERATURE", "MYSTERY", "NON-FICTION", "PARENTING", "POLITICS",
                    "REFERENCE", "RELIGION", "ROMANCE", "SELF-HELP", "SCIENCE-NATURE", "SCIENCE-FICTION", "SPORTS",
                    "YOUTH", "TRAVEL" };
            String[] backings = { "HARDBACK", "PAPERBACK", "USED", "AUDIO", "LIMITED-EDITION" };

            String tableName = TPCWNamingScheme.getItemsTableName();

            System.out.println("Inserting Items: " + n);

            ArrayList<String> titles = new ArrayList<String>();
            for (int i = 0; i < n; i++) {

                String title = BenchmarkUtil.getRandomAString(14, 60);
                // int num = rand.nextInt(1000);
                titles.add(title);
            }
            int lastStart = -1;
            for (int i = 0; i < n; i++) {

                if ((i % TX_SIZE) == 0) {
                    lastStart = i;
                    try {
                        ((TPCW_SwiftCloud_Executor) client).startTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }
                }

                String I_TITLE;

                Author I_AUTHOR;
                String I_PUBLISHER;
                String I_DESC;
                String I_SUBJECT;
                float I_COST;
                int I_STOCK;
                int[] I_RELATED = new int[5];
                int I_PAGE;
                String I_BACKING;
                I_TITLE = titles.get(i);

                int author_pos = rand.nextInt(authors.size());

                I_AUTHOR = authors.get(author_pos);
                int author = I_AUTHOR.getAuthor_id();

                // I_AUTHOR = author;//(BenchmarkUtil.getRandomAString(8, 15) +
                // " " + BenchmarkUtil.getRandomAString(8, 15));
                // insert(I_AUTHOR, I_TITLE, column_family, "I_AUTHOR",
                // writeCon);

                I_PUBLISHER = BenchmarkUtil.getRandomAString(14, 60);
                // insert(I_PUBLISHER, I_TITLE, column_family, "I_PUBLISHER",
                // writeCon);

                boolean rad1 = rand.nextBoolean();
                I_DESC = null;
                if (rad1) {
                    I_DESC = BenchmarkUtil.getRandomAString(100, 500);
                    // insert(I_DESC, I_TITLE, column_family, "I_DESC",
                    // writeCon);
                }

                I_COST = rand.nextInt(100);
                // insert(I_AUTHOR, I_TITLE, column_family, "I_AUTHOR",
                // writeCon);

                I_STOCK = BenchmarkUtil.getRandomInt(10, 30);
                // insert(I_STOCK, I_TITLE, column_family, "I_STOCK", writeCon);

                for (int z = 0; z < 5; z++) {
                    I_RELATED[z] = rand.nextInt(NUM_ITEMS);
                }

                I_PAGE = rand.nextInt(500) + 10;
                // insert(I_PAGE, I_TITLE, column_family, "I_PAGE", writeCon);

                I_SUBJECT = subjects[rand.nextInt(subjects.length)];
                // insert(I_SUBJECT, I_TITLE, column_family, "I_SUBJECT",
                // writeCon);

                I_BACKING = backings[rand.nextInt(backings.length)];
                // insert(I_BACKING, I_TITLE, column_family, "I_BACKING",
                // writeCon);

                // GregorianCalendar cal = BenchmarkUtil.getRandomDate(1930,
                // 2000);

                long pubDate = System.currentTimeMillis();

                String thumbnail = new String("img" + i % 100 + "/thumb_" + i + ".gif");
                String image = new String("img" + i % 100 + "/image_" + i + ".gif");

                double srp = (double) BenchmarkUtil.getRandomInt(100, 99999);
                srp /= 100.0;

                String isbn = BenchmarkUtil.getRandomAString(13);

                long avail = System.currentTimeMillis() + rand.nextInt(1200000);

                String dimensions = ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0) + "x"
                        + ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0) + "x"
                        + ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0);

                Item item = new Item(base + i, I_TITLE, pubDate, I_PUBLISHER, I_DESC, I_SUBJECT, thumbnail, image,
                        I_COST, isbn, srp, I_RELATED[0], I_RELATED[1], I_RELATED[2], I_RELATED[3], I_RELATED[4],
                        I_PAGE, avail, I_BACKING, dimensions, author);

                try {
                    databaseInsert(item, tableName, (base + i) + "", I_STOCK);
                } catch (Exception e) {
                    e.printStackTrace();
                    client_error = true;
                    break;

                }
                if (build_indexes) {
                    Map<String, Object> index_values = new TreeMap<String, Object>();
                    index_values.put("A_FNAME", I_AUTHOR.getA_FNAME());
                    index_values.put("A_LNAME", I_AUTHOR.getA_LNAME());
                    index_values.put("I_TITLE", I_TITLE);

                    long time_stamp = Long.MAX_VALUE - pubDate;

                    try {
                        client.index(I_SUBJECT, "item_subject_index", (time_stamp) + "." + (base + i), index_values);
                        client.index(I_TITLE, "item_title_index", (base + i) + "", index_values);
                        client.index(I_AUTHOR.getA_LNAME(), "item_author_index", (base + i) + "", index_values);
                    } catch (Exception e) {
                        e.printStackTrace();
                        client_error = true;
                        break;

                    }
                }
                partial_items.add(item.getI_ID());

                if ((i + 1) - lastStart == TX_SIZE || i + 1 == n)
                    try {
                        ((TPCW_SwiftCloud_Executor) client).endTransactionForThread();
                    } catch (NetworkException e) {
                        e.printStackTrace();
                    }

            }

            if (debug) {
                System.out.println("Thread finished: " + num_items + " items inserted");
            }

            barrier.countDown();
            client.closeClient();
        }

        public void databaseInsert(Item item, String bucket, String key, int stock) throws Exception {
            ((TPCW_SwiftCloud_Executor) client).insertItem(key, item, stock);

        }

        public void databaseInsert(String Operation, String key, String path, Entity value, ResultHandler results)
                throws Exception {

            long time1 = System.currentTimeMillis();
            client.insert(key, path, value);
            long time2 = System.currentTimeMillis();
            results.logResult(Operation, time2 - time1);

        }

        public ArrayList<Integer> getData() {
            return partial_items;
        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

    /**
     * *********** Addresses* ***********
     */
    public void insertAddresses(int n, boolean insert) throws InterruptedException {

        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        System.out.println(">>Inserting " + n + " Addresses || populatores " + num_threads);

        barrier = new CountDownLatch(threads);
        AddressPopulator[] partial_addresses = new AddressPopulator[threads];
        for (int i = threads; i > 0; i--) {

            int base = (threads - i) * sections;

            AddressPopulator populator = null;
            if (i == 0) {
                populator = new AddressPopulator(firstSection, insert, base);

            } else {
                populator = new AddressPopulator(sections, insert, base);
            }
            Thread t = new Thread(populator);
            partial_addresses[threads - i] = populator;
            t.start();
        }
        barrier.await();

        for (AddressPopulator populator : partial_addresses) {

            ArrayList<String> ids = populator.getData();
            for (String id : ids) {
                addresses.add(id);
            }
            if (insert)
                results.addResults(populator.returnResults());
            populator.partial_results.cleanResults();
            populator = null;
        }
        partial_addresses = null;
        System.gc();

    }

    class AddressPopulator implements Runnable {

        int num_addresses;
        DatabaseExecutorInterface client;
        ArrayList<String> partial_adresses;
        ResultHandler partial_results;
        boolean insertDB;
        int base = 0;

        public AddressPopulator(int num_addresses, boolean insertDB, int base) {
            client = databaseClientFactory.getDatabaseClient();
            this.num_addresses = num_addresses;
            partial_adresses = new ArrayList<String>();
            partial_results = new ResultHandler("", rounds);
            this.insertDB = insertDB;
            this.base = base;
        }

        public void run() {
            this.insertAddress(num_addresses);
        }

        public void databaseInsert(String Operation, String key, String path, Entity value, ResultHandler results)
                throws Exception {

            long time1 = System.currentTimeMillis();
            client.insert(key, path, value);
            long time2 = System.currentTimeMillis();
            results.logResult(Operation, time2 - time1);

        }

        private void insertAddress(int n) {

            System.out.println("Inserting Address: " + n);

            String ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE;
            String ADDR_ZIP;
            int country_id;
            int lastStart = -1;
            for (int i = 0; i < n; i++) {

                if ((i % TX_SIZE) == 0) {
                    lastStart = i;
                    try {
                        ((TPCW_SwiftCloud_Executor) client).startTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }
                }
                ADDR_STREET1 = "street" + BenchmarkUtil.getRandomAString(10, 30);

                ADDR_STREET2 = "street" + BenchmarkUtil.getRandomAString(10, 30);
                ADDR_CITY = BenchmarkUtil.getRandomAString(4, 30);
                ADDR_STATE = BenchmarkUtil.getRandomAString(2, 20);
                ADDR_ZIP = BenchmarkUtil.getRandomAString(5, 10);
                country_id = countries.get(BenchmarkUtil.getRandomInt(0, NUM_COUNTRIES - 1));

                String key = ADDR_STREET1 + ADDR_STREET2 + ADDR_CITY + ADDR_STATE + ADDR_ZIP + country_id;

                Address address = new Address(key, ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE, ADDR_ZIP,
                        country_id);
                // insert(ADDR_STREET1, key, "Addresses", "ADDR_STREET1",
                // writeConsistency);
                // insert(ADDR_STREET2, key, "Addresses", "ADDR_STREET2",
                // writeConsistency);
                // insert(ADDR_STATE, key, "Addresses", "ADDR_STATE",
                // writeConsistency);
                // insert(ADDR_CITY, key, "Addresses", "ADDR_CITY",
                // writeConsistency);
                // insert(ADDR_ZIP, key, "Addresses", "ADDR_ZIP",
                // writeConsistency);
                // insert(country.getCo_id(), key, "Addresses", "ADDR_CO_ID",
                // writeConsistency);

                if (insertDB) {
                    try {
                        databaseInsert("INSERT_Addresses", key + "", TPCWNamingScheme.getAddressTableName(), address,
                                partial_results);
                    } catch (Exception e) {
                        e.printStackTrace();
                        client_error = true;
                        break;

                    }
                }
                partial_adresses.add(key);

                if ((i + 1) - lastStart == TX_SIZE || i + 1 == n)
                    try {
                        ((TPCW_SwiftCloud_Executor) client).endTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }

            }
            if (debug) {
                System.out.println("Thread finished: " + num_addresses + " addresses.");
            }

            barrier.countDown();
            client.closeClient();
        }

        public ArrayList<String> getData() {
            return partial_adresses;
        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

    /**
     * ******** Countries * *********
     */
    private void insertCountries(int numCountries) {

        DatabaseExecutorInterface client;
        client = databaseClientFactory.getDatabaseClient();

        String[] countriesNames = { "United States", "United Kingdom", "Canada", "Germany", "France", "Japan",
                "Netherlands", "Italy", "Switzerland", "Australia", "Algeria", "Argentina", "Armenia", "Austria",
                "Azerbaijan", "Bahamas", "Bahrain", "Bangla Desh", "Barbados", "Belarus", "Belgium", "Bermuda",
                "Bolivia", "Botswana", "Brazil", "Bulgaria", "Cayman Islands", "Chad", "Chile", "China",
                "Christmas Island", "Colombia", "Croatia", "Cuba", "Cyprus", "Czech Republic", "Denmark",
                "Dominican Republic", "Eastern Caribbean", "Ecuador", "Egypt", "El Salvador", "Estonia", "Ethiopia",
                "Falkland Island", "Faroe Island", "Fiji", "Finland", "Gabon", "Gibraltar", "Greece", "Guam",
                "Hong Kong", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel",
                "Jamaica", "Jordan", "Kazakhstan", "Kuwait", "Lebanon", "Luxembourg", "Malaysia", "Mexico",
                "Mauritius", "New Zealand", "Norway", "Pakistan", "Philippines", "Poland", "Portugal", "Romania",
                "Russia", "Saudi Arabia", "Singapore", "Slovakia", "South Africa", "South Korea", "Spain", "Sudan",
                "Sweden", "Taiwan", "Thailand", "Trinidad", "Turkey", "Venezuela", "Zambia" };

        double[] exchanges = { 1, .625461, 1.46712, 1.86125, 6.24238, 121.907, 2.09715, 1842.64, 1.51645, 1.54208,
                65.3851, 0.998, 540.92, 13.0949, 3977, 1, .3757, 48.65, 2, 248000, 38.3892, 1, 5.74, 4.7304, 1.71,
                1846, .8282, 627.1999, 494.2, 8.278, 1.5391, 1677, 7.3044, 23, .543, 36.0127, 7.0707, 15.8, 2.7, 9600,
                3.33771, 8.7, 14.9912, 7.7, .6255, 7.124, 1.9724, 5.65822, 627.1999, .6255, 309.214, 1, 7.75473,
                237.23, 74.147, 42.75, 8100, 3000, .3083, .749481, 4.12, 37.4, 0.708, 150, .3062, 1502, 38.3892, 3.8,
                9.6287, 25.245, 1.87539, 7.83101, 52, 37.8501, 3.9525, 190.788, 15180.2, 24.43, 3.7501, 1.72929,
                43.9642, 6.25845, 1190.15, 158.34, 5.282, 8.54477, 32.77, 37.1414, 6.1764, 401500, 596, 2447.7 };

        String[] currencies = { "Dollars", "Pounds", "Dollars", "Deutsche Marks", "Francs", "Yen", "Guilders", "Lira",
                "Francs", "Dollars", "Dinars", "Pesos", "Dram", "Schillings", "Manat", "Dollars", "Dinar", "Taka",
                "Dollars", "Rouble", "Francs", "Dollars", "Boliviano", "Pula", "Real", "Lev", "Dollars", "Franc",
                "Pesos", "Yuan Renmimbi", "Dollars", "Pesos", "Kuna", "Pesos", "Pounds", "Koruna", "Kroner", "Pesos",
                "Dollars", "Sucre", "Pounds", "Colon", "Kroon", "Birr", "Pound", "Krone", "Dollars", "Markka", "Franc",
                "Pound", "Drachmas", "Dollars", "Dollars", "Forint", "Krona", "Rupees", "Rupiah", "Rial", "Dinar",
                "Punt", "Shekels", "Dollars", "Dinar", "Tenge", "Dinar", "Pounds", "Francs", "Ringgit", "Pesos",
                "Rupees", "Dollars", "Kroner", "Rupees", "Pesos", "Zloty", "Escudo", "Leu", "Rubles", "Riyal",
                "Dollars", "Koruna", "Rand", "Won", "Pesetas", "Dinar", "Krona", "Dollars", "Baht", "Dollars", "Lira",
                "Bolivar", "Kwacha" };

        if (numCountries > countriesNames.length) {
            numCountries = countriesNames.length - 1;
        }

        System.out.println(">>Inserting " + numCountries + " countries || populatores " + num_threads);

        int lastStart = -1;
        for (int i = 0; i < numCountries; i++) {

            if (i % TX_SIZE == 0) {
                try {
                    lastStart = i;
                    ((TPCW_SwiftCloud_Executor) client).startTransactionForThread();
                } catch (NetworkException e1) {
                    e1.printStackTrace();
                }
            }

            // Country name = key
            // insert(exchanges[i], countriesNames[i], "Countries",
            // "CO_EXCHANGE", writeConsitency);
            // insert(currencies[i], countriesNames[i], "Countries",
            // "CO_CURRENCY", writeConsitency);

            Country country = new Country(i, countriesNames[i], currencies[i], exchanges[i]);
            try {
                databaseInsert(client, "INSERT_Countries", i + "", TPCWNamingScheme.getCountryTableName(), country,
                        results);
            } catch (Exception e) {
                e.printStackTrace();
                client_error = true;
                break;
            }
            this.countries.add(i);

            if ((i + 1) - lastStart == TX_SIZE || i + 1 == numCountries)
                try {
                    ((TPCW_SwiftCloud_Executor) client).endTransactionForThread();
                } catch (NetworkException e1) {
                    e1.printStackTrace();
                }

        }

        if (debug) {
            System.out.println("Countries:" + countriesNames.length + " inserted");
        }
    }

    /**
     * **************** Order and XACTS * ******************
     */
    public void insertOrder_and_CC_XACTS(int n) throws InterruptedException {

        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        System.out.println(">>Inserting " + n + " Orders || populatores " + num_threads);

        barrier = new CountDownLatch(threads);

        Order_and_XACTSPopulator[] partial_orders = new Order_and_XACTSPopulator[threads];
        for (int i = threads; i > 0; i--) {

            int base = (threads - i); // /code copy form above constructors, if
                                      // reactivated please revise

            Order_and_XACTSPopulator populator = null;
            if (i == 0) {
                populator = new Order_and_XACTSPopulator(firstSection, base);

            } else {
                populator = new Order_and_XACTSPopulator(sections, base);
            }
            partial_orders[threads - i] = populator;
            Thread t = new Thread(populator, "Order populator" + (threads - i));
            t.start();
        }
        barrier.await();

        System.out.println("END");

        for (Order_and_XACTSPopulator populator : partial_orders) {
            results.addResults(populator.returnResults());
            populator.partial_results.cleanResults();
            populator = null;
        }
        System.gc();

    }

    class Order_and_XACTSPopulator implements Runnable {

        int num_orders;
        int base = 0;
        DatabaseExecutorInterface client;
        ResultHandler partial_results;

        public Order_and_XACTSPopulator(int num_orders, int base) {
            client = databaseClientFactory.getDatabaseClient();
            this.num_orders = num_orders;
            partial_results = new ResultHandler("", rounds);
            this.base = base;
        }

        public void run() {
            this.insertOrder_and_CC_XACTS(num_orders);
        }

        public void databaseInsert(Order order, List<OrderLine> orderLines, CCXactItem ccXact) throws Exception {
            ((TPCW_SwiftCloud_Executor) client).insertOrder(order, orderLines, ccXact);

        }

        public void insertOrder_and_CC_XACTS(int number_keys) {
            System.out.println("Inserting Order: " + number_keys);
            // String table = "Order";
            String[] credit_cards = { "VISA", "MASTERCARD", "DISCOVER", "AMEX", "DINERS" };
            String[] ship_types = { "AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL" };
            String[] status_types = { "PROCESSING", "SHIPPED", "PENDING", "DENIED" };
            //
            // long O_ID = begin_key;
            // // ColumnPath path = new ColumnPath(column_family);
            // // path.setSuper_column("ids".getBytes());
            //
            NodeKeyGenerator nodeKeyGenerator = new NodeKeyGenerator(0);
            int lastStart = -1;
            for (int z = 0; z < number_keys; z++) {

                if (z % TX_SIZE == 0) {
                    lastStart = z;
                    try {
                        ((TPCW_SwiftCloud_Executor) client).startTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }
                }
                // table = "ORDERS";

                String O_C_ID;
                long O_DATE;
                float O_SUB_TOTAL;
                float O_TAX;
                float O_TOTAL;
                long O_SHIP_DATE;
                String O_SHIP_TYPE;
                String O_SHIP_ADDR;
                String O_STATUS;

                String Customer_id = customers.get(rand.nextInt(customers.size()));

                O_C_ID = Customer_id;

                GregorianCalendar call = new GregorianCalendar();
                O_DATE = call.getTimeInMillis();
                // insertInSuperColumn(O_DATE, O_C_ID, column_family, O_ID + "",
                // "O_DATE", write_con);

                O_SUB_TOTAL = rand.nextFloat() * 100 * 4;
                // insertInSuperColumn(O_SUB_TOTAL, O_C_ID, column_family, O_ID
                // + "", "O_SUB_TOTAL", write_con);

                O_TAX = O_SUB_TOTAL * 0.21f;
                // insertInSuperColumn(O_TAX, O_C_ID, column_family, O_ID + "",
                // "O_TAX", write_con);

                O_TOTAL = O_SUB_TOTAL + O_TAX;
                // insertInSuperColumn(O_TOTAL, O_C_ID, column_family, O_ID +
                // "", "O_TOTAL", write_con);

                call.add(Calendar.DAY_OF_YEAR, -1 * rand.nextInt(60) + 1);
                O_SHIP_DATE = call.getTimeInMillis();
                // insertInSuperColumn(O_SHIP_DATE, O_C_ID, column_family, O_ID
                // + "", "O_SHIP_DATE", write_con);

                O_SHIP_TYPE = ship_types[rand.nextInt(ship_types.length)];
                // insertInSuperColumn(O_SHIP_TYPE, O_C_ID, column_family, O_ID
                // + "", "O_SHIP_TYPE", write_con);

                O_STATUS = status_types[rand.nextInt(status_types.length)];
                // insertInSuperColumn(O_STATUS, O_C_ID, column_family, O_ID +
                // "", "O_STATUS", write_con);

                String billAddress = addresses.get(BenchmarkUtil.getRandomInt(0, NUM_ADDRESSES - 1));
                // insertInSuperColumn(billAddress.getAddr_id(), O_C_ID,
                // column_family, O_ID + "", "O_BILL_ADDR_ID", write_con);

                O_SHIP_ADDR = addresses.get(BenchmarkUtil.getRandomInt(0, NUM_ADDRESSES - 1));
                // insertInSuperColumn(O_SHIP_ADDR.getAddr_id(), O_C_ID,
                // column_family, O_ID + "", "O_SHIP_ADDR_ID", write_con);

                String order_id = (String) nodeKeyGenerator.getNextKey(base);

                Order order = new Order(order_id, O_C_ID, O_DATE, O_SUB_TOTAL, O_TAX, O_TOTAL, O_SHIP_TYPE,
                        O_SHIP_DATE, O_STATUS, billAddress, O_SHIP_ADDR);

                List<OrderLine> orderLines = new ArrayList<OrderLine>();
                int number_of_items = rand.nextInt(4) + 1;
                //

                for (int i = 0; i < number_of_items; i++) {
                    /**
                     * OL_ID OL_O_ID OL_I_ID OL_QTY OL_DISCOUNT OL_COMMENT
                     */
                    String OL_ID;

                    int OL_I_ID;
                    int OL_QTY;
                    float OL_DISCOUNT;
                    String OL_COMMENT;

                    OL_ID = order_id + "." + i;

                    OL_I_ID = items.get(rand.nextInt(items.size()));

                    OL_QTY = rand.nextInt(4) + 1;

                    OL_DISCOUNT = (float) rand.nextInt(30) / 100f;

                    OL_COMMENT = null;

                    OL_COMMENT = BenchmarkUtil.getRandomAString(20, 100);

                    OrderLine orderline = new OrderLine(OL_ID, order_id, OL_I_ID, OL_QTY, OL_DISCOUNT, OL_COMMENT);
                    orderLines.add(orderline);
                }
                //
                //
                String CX_TYPE;
                int CX_NUM;
                String CX_NAME;
                long CX_EXPIRY;
                // double CX_XACT_AMT;
                // int CX_CO_ID; // Order.getID;

                // table = "CC_XACTS";

                CX_NUM = BenchmarkUtil.getRandomNString(16);

                CX_TYPE = credit_cards[BenchmarkUtil.getRandomInt(0, credit_cards.length - 1)];
                // insert(CX_TYPE, key, column_family, "CX_TYPE", write_con);

                // insert(CX_NUM, key, column_family, "CX_NUM", write_con);

                CX_NAME = BenchmarkUtil.getRandomAString(14, 30);
                // insert(CX_NAME, key, column_family, "CX_NAME", write_con);

                GregorianCalendar cal = new GregorianCalendar();
                cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(10, 730));
                CX_EXPIRY = cal.getTimeInMillis();
                // insert(CX_EXPIRY, key, column_family, "CX_EXPIRY",
                // write_con);

                // DATE
                // insert(O_SHIP_DATE, key, column_family, "CX_XACT_DATE",
                // write_con);

                // AMOUNT
                // insert(O_TOTAL, key, column_family, "CX_XACT_AMT",
                // write_con);

                // CX_AUTH_ID = getRandomAString(5,15);// unused
                int country_id = countries.get(BenchmarkUtil.getRandomInt(0, countries.size() - 1));
                // insert(country.getCo_id(), key, column_family, "CX_CO_ID",
                // write_con);

                CCXactItem ccXact = new CCXactItem(CX_TYPE, CX_NUM, CX_NAME, CX_EXPIRY, O_TOTAL, O_SHIP_DATE, order_id,
                        country_id);

                try {
                    databaseInsert(order, orderLines, ccXact);

                } catch (Exception e) {
                    e.printStackTrace();
                    client_error = true;
                    break;

                }

                if ((z + 1) - lastStart == TX_SIZE || z + 1 == number_keys)
                    try {
                        ((TPCW_SwiftCloud_Executor) client).endTransactionForThread();
                    } catch (NetworkException e1) {
                        e1.printStackTrace();
                    }

                // O_ID++;
            }
            if (debug) {
                System.out.println("Thread finished: " + number_keys + " orders and xact inserted.");
            }
            //

            barrier.countDown();
            client.closeClient();

        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

}

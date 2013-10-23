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

package org.uminho.gsd.benchmarks.generic.workloads;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkMain;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkNodeID;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.generic.Constants;
import org.uminho.gsd.benchmarks.helpers.ProgressBar;
import org.uminho.gsd.benchmarks.interfaces.ProbabilityDistribution;
import org.uminho.gsd.benchmarks.interfaces.Workload.Operation;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;

public class TPCWWorkloadGeneration implements WorkloadGeneratorInterface {

    Logger logger = Logger.getLogger(TPCWWorkloadGeneration.class);

    // The probability distribution
    private ProbabilityDistribution distribution;

    /**
     * Workload Values
     */
    Map<String, Double> workload_values;

    // This client number
    private int private_id;

    boolean create = true;

    // Subjects to be search in the database
    String[] subjects = { "ARTS", "BIOGRAPHIES", "BUSINESS", "CHILDREN", "COMPUTERS", "COOKING", "HEALTH", "HISTORY",
            "HOME", "HUMOR", "LITERATURE", "MYSTERY", "NON-FICTION", "PARENTING", "POLITICS", "REFERENCE", "RELIGION",
            "ROMANCE", "SELF-HELP", "SCIENCE-NATURE", "SCIENCE-FICTION", "SPORTS", "YOUTH", "TRAVEL" };

    String[] search_types = { "SUBJECT", "TITLE", "AUTHOR" };

    ProgressBar progressBar;

    private Random rand;

    private BenchmarkNodeID nodeID;

    double buy_request_prob = 1;
    double buy_confirm_prob = 1;

    int customer_id = Constants.NUM_CUSTOMERS + 1;

    ResultHandler resultHandler;

    int shop_id = 0;

    // The authors names
    List<String> author_names;

    // The existing titles
    List<String> item_titles;

    List<Integer> item_cache;

    public TPCWWorkloadGeneration(ResultHandler handler, List<String> items, List<String> authors,
            Map<String, Double> workload, ProbabilityDistribution distribution, BenchmarkNodeID nodeID,
            int personal_number, ProgressBar progressBar) {

        this.resultHandler = handler;
        this.item_titles = items;
        Constants.NUM_ITEMS = items.size();

        this.author_names = authors;
        this.workload_values = new TreeMap<String, Double>();

        for (String operation : workload.keySet()) {
            this.workload_values.put(operation, workload.get(operation));

        }

        this.distribution = distribution.getNewInstance();

        this.progressBar = progressBar;

        this.nodeID = nodeID;

        this.private_id = personal_number;

        rand = new Random();

        // home
        // shoppingCart
        // register/login
        // buy_request
        // buy_confirm
        // order_inquiry
        // search
        // new_products
        // best_sellers
        // product_detail
        // admin_change

        // prepare probabilities
        double aggregated_probability = 0;
        aggregated_probability = workload_values.get("home");

        double prob = workload_values.get("new_products");
        workload_values.put("new_products", prob + aggregated_probability);
        aggregated_probability += prob;

        prob = workload_values.get("best_sellers");
        workload_values.put("best_sellers", prob + aggregated_probability);
        aggregated_probability += prob;

        prob = workload_values.get("product_detail");
        workload_values.put("product_detail", prob + aggregated_probability);
        aggregated_probability += prob;

        prob = workload_values.get("search");
        workload_values.put("search", prob + aggregated_probability);
        aggregated_probability += prob;

        prob = workload_values.get("register/login");
        workload_values.put("register/login", prob + aggregated_probability);
        aggregated_probability += prob;

        prob = workload_values.get("order_inquiry");
        workload_values.put("order_inquiry", prob + aggregated_probability);
        aggregated_probability += prob;

        prob = workload_values.get("admin_change");
        workload_values.put("admin_change", prob + aggregated_probability);
        aggregated_probability += prob;

        prob = workload_values.get("shoppingCart");
        workload_values.put("shoppingCart", prob + aggregated_probability);
        aggregated_probability += prob;

        this.item_cache = Collections.synchronizedList(new LinkedList<Integer>());
        this.itemStats = new ConcurrentHashMap<Integer, Integer>();

        Set<Integer> randomItems = new HashSet<Integer>();

        for (int i = 0; i < BenchmarkMain.repetitionSetSize;) {
            int randi = rand.nextInt(Constants.NUM_ITEMS);
            if (!randomItems.contains(rand)) {
                item_cache.add(randi);
                randomItems.add(randi);
                i++;
            }
        }

        // //cheats
        // double rest = 100 - aggregated_probability;
        // prob = workload.get("shoppingCart");
        // workload_values.put("shoppingCart", 100d);

        // set buy options probabilities
        double sc_prob = workload.get("shoppingCart");
        double br_prob = workload.get("buy_request");
        buy_request_prob = br_prob / sc_prob;
        double bc_prob = workload.get("buy_confirm");
        buy_confirm_prob = bc_prob / br_prob;
    }

    // home
    // shoppingCart
    // register/login
    // buy_request
    // buy_confirm
    // order_inquiry
    // search
    // new_products
    // best_sellers
    // product_detail
    // admin_change

    int i1 = 0;
    int i2 = 0;
    int i3 = 0;
    int i4 = 0;
    int i5 = 0;
    int i6 = 0;
    int i7 = 0;
    int i8 = 0;
    int i9 = 0;
    int i10 = 0;
    int i11 = 0;

    String subject = "AUTHOR";// "SUBJECT","TITLE"

    boolean shopping = false;
    boolean confirm = false;
    int operation_num = 0;
    boolean miss_print = false;
    final long txnStartTime = System.currentTimeMillis();
    final AtomicBoolean done = new AtomicBoolean(false);

    private Map<Integer, Integer> itemStats;

    public Operation getNextOperation() {

        if (!miss_print) {
            progressBar.increment(private_id);
            operation_num++;
        } else {
            miss_print = false;
        }

        Operation op = new Operation("ERROR", null);
        printStats();

        if (shopping) {

            if (confirm) {

                i11++;
                shopping = false;
                confirm = false;

                Map<String, Object> parametros = new TreeMap<String, Object>();

                parametros.put("CART", nodeID.getId() + "." + private_id + "." + shop_id);
                parametros.put("CUSTOMER", rand.nextInt(Constants.NUM_CUSTOMERS) + "");

                op = new Operation("OP_BUY_CONFIRM", parametros);

                shop_id++;
                create = true;
                return op;

            } else {

                Map<String, Object> parametros = new TreeMap<String, Object>();
                parametros.put("CART", nodeID.getId() + "." + private_id + "." + shop_id);
                op = new Operation("OP_BUY_REQUEST", parametros);

                i10++;
                double d = rand.nextDouble();
                if (d < buy_confirm_prob) {
                    confirm = true;

                } else {
                    shopping = false;
                    confirm = false;
                }
            }
        } else {
            double d = rand.nextDouble() * 100;
            if (d < workload_values.get("home")) {

                Map<String, Object> parametros = new TreeMap<String, Object>();
                int item = getItemId();
                parametros.put("ITEM", item);
                parametros.put("COSTUMER", rand.nextInt(Constants.NUM_CUSTOMERS));

                op = new Operation("OP_HOME", parametros);

                i1++;

            } else if (d < workload_values.get("new_products")) {

                i2++;

                String subject_field = subjects[rand.nextInt(subjects.length)];

                Map<String, Object> parametros = new TreeMap<String, Object>();
                parametros.put("FIELD", subject_field);

                op = new Operation("OP_NEW_PRODUCTS", parametros);
                return op;

            } else if (d < workload_values.get("best_sellers")) {
                i3++;
                String subject_field = subjects[rand.nextInt(subjects.length)];

                Map<String, Object> parametros = new TreeMap<String, Object>();
                parametros.put("FIELD", subject_field);

                op = new Operation("OP_BEST_SELLERS", parametros);

                return op;

            } else if (d < workload_values.get("product_detail")) {
                i4++;

                int item = getItemId();
                Map<String, Object> parametros = new TreeMap<String, Object>();
                parametros.put("ITEM", item);

                op = new Operation("OP_ITEM_INFO", parametros);
                return op;

            } else if (d < workload_values.get("search")) {
                i5++;
                subject = search_types[rand.nextInt(search_types.length)];

                if (subject.equals("AUTHOR")) {
                    String subject_field = author_names.get(rand.nextInt(author_names.size()));

                    Map<String, Object> parametros = new TreeMap<String, Object>();

                    parametros.put("TERM", subject);
                    parametros.put("FIELD", subject_field);

                    op = new Operation("OP_SEARCH", parametros);
                    return op;

                } else if (subject.equals("TITLE")) {
                    String subject_field = item_titles.get(rand.nextInt(item_titles.size()));

                    Map<String, Object> parametros = new TreeMap<String, Object>();

                    parametros.put("TERM", subject);
                    parametros.put("FIELD", subject_field);

                    op = new Operation("OP_SEARCH", parametros);
                    return op;

                } else if (subject.equals("SUBJECT")) {
                    String subject_field = subjects[rand.nextInt(subjects.length)];

                    Map<String, Object> parametros = new TreeMap<String, Object>();

                    parametros.put("TERM", subject);
                    parametros.put("FIELD", subject_field);

                    op = new Operation("OP_SEARCH", parametros);
                    return op;

                } else {

                    System.out.println("OPTION NOT RECOGNIZED");
                }

            } else if (d < workload_values.get("register/login")) {
                Map<String, Object> parametros = new TreeMap<String, Object>();

                float decision_factor = rand.nextFloat();
                if (decision_factor < 0.2) {
                    parametros.put("CUSTOMER", nodeID.getId() + "." + private_id + "." + customer_id);
                    op = new Operation("OP_REGISTER", parametros);
                    customer_id++;
                } else {
                    parametros.put("CUSTOMER", rand.nextInt(Constants.NUM_CUSTOMERS) + "");
                    op = new Operation("OP_LOGIN", parametros);
                }

                i6++;

            } else if (d < workload_values.get("order_inquiry")) {
                i7++;
                Map<String, Object> parametros = new TreeMap<String, Object>();
                parametros.put("CUSTOMER", rand.nextInt(Constants.NUM_CUSTOMERS) + "");
                op = new Operation("OP_ORDER_INQUIRY", parametros);

            } else if (d < workload_values.get("admin_change")) {

                i8++;
                int item = rand.nextInt(Constants.NUM_ITEMS);
                Map<String, Object> parametros = new TreeMap<String, Object>();
                parametros.put("ITEM", item);
                op = new Operation("OP_ADMIN_CHANGE", parametros);

                return op;

            } else if (d <= workload_values.get("shoppingCart")) {

                Map<String, Object> parametros = new TreeMap<String, Object>();
                parametros.put("CART", nodeID.getId() + "." + private_id + "." + shop_id);

                int item = distribution.getNextElement();

                parametros.put("ITEM", item);

                // parametros.put("ITEM", rand.nextInt(Constants.NUM_ITEMS));
                parametros.put("CREATE", create);

                op = new Operation("OP_SHOPPING_CART", parametros);

                create = false;

                i9++;
                double d2 = rand.nextDouble();
                if (d2 < buy_request_prob) {
                    shopping = true;
                    confirm = false;
                } else {
                    shopping = false;
                    confirm = false;
                }

            } else {
                while (op.getOperation().equals("ERROR")) {
                    miss_print = true;
                    op = getNextOperation();
                }
            }

        }
        if (op.getOperation().equals("ERROR")) {
            System.out.println("ERROR");

        }

        return op;
    }

    private int getItemId() {
        double localItemRand = rand.nextDouble();
        int item;
        if (localItemRand < BenchmarkMain.repetitionFrequency) {
            item = item_cache.remove(rand.nextInt(item_cache.size()));
            item_cache.add(item);
        } else {
            item = rand.nextInt(Constants.NUM_ITEMS);

            if (BenchmarkMain.repetitionFrequency > 0) {
                item_cache.remove(0);
                item_cache.add(item);
            }
        }

        Integer itemCount = itemStats.get(item);
        if (itemCount == null) {
            itemStats.put(item, 1);
        } else {
            itemStats.put(item, itemCount + 1);
        }
        return item;
    }

    public void printStats() {

        if (operation_num == progressBar.getTop_limit()) {
            resultHandler.concurrent_countEvent("WORKLOAD", "HOME", i1);
            resultHandler.concurrent_countEvent("WORKLOAD", "NEW PRODUCTS", i2);
            resultHandler.concurrent_countEvent("WORKLOAD", "BEST SELLER", i3);
            resultHandler.concurrent_countEvent("WORKLOAD", "PRODUCT DETAIL", i4);
            resultHandler.concurrent_countEvent("WORKLOAD", "SEARCH", i5);
            resultHandler.concurrent_countEvent("WORKLOAD", "COSTUMER REGISTRATION", i6);
            resultHandler.concurrent_countEvent("WORKLOAD", "ORDER INQUIRY", i7);
            resultHandler.concurrent_countEvent("WORKLOAD", "ADMIN ACTION", i8);
            resultHandler.concurrent_countEvent("WORKLOAD", "SHOPPING CART", i9);
            resultHandler.concurrent_countEvent("WORKLOAD", "BUY REQUEST", i10);
            resultHandler.concurrent_countEvent("WORKLOAD", "BUY CONFIRM", i10);

            System.out.println(nodeID.getId() + " " + itemStats);

            // System.out.println("-------");
            // System.out.println("OP1 H:" + ((i1 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP2 NP:" + ((i2 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP3 BS:" + ((i3 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP4 PD:" + ((i4 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP5 S:" + ((i5 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP6 CR:" + ((i6 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP7 OI:" + ((i7 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP8 AC:" + ((i8 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP9 SC:" + ((i9 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP10 BR:" + ((i10 / (operation_num * 1d)) *
            // 100));
            // System.out.println("OP11 BC:" + ((i11 / (operation_num * 1d)) *
            // 100));
            // System.out.println("-------");
        }

    }

}
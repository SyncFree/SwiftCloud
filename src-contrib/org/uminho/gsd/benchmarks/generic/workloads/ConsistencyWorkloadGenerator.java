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


import org.uminho.gsd.benchmarks.helpers.ProgressBar;
import org.uminho.gsd.benchmarks.interfaces.ProbabilityDistribution;
import org.uminho.gsd.benchmarks.interfaces.Workload.Operation;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;

import java.util.*;


public class ConsistencyWorkloadGenerator implements WorkloadGeneratorInterface {

    //The items and their stock in the database
    private List<String> items;
    //The probability distribution
    private ProbabilityDistribution distribution;
    //This clientID
    private String clientID;

    //items to buy in this iteration
    private Queue<String> items_to_buy;

    //Waiting for stock;
    private boolean watching_item;
    //Last fetched stock
    private Operation last_stock;
    //Last fetched item;
    private String last_item;

    //This client number
    private int private_id;

    //Number of pooled elements form queue;
    private int debug_number;

    //Last used cart
    private int cart;

	private int mean_cart_items = ConsistencyTestWorkloadFactory.mean_cart_items;


    ProgressBar progressBar;

    private Random rand;

    public ConsistencyWorkloadGenerator(List<String> items, ProbabilityDistribution distribution, String clientID, int personal_number, ProgressBar progressBar) {

		HashSet<Integer> sorted_items = new HashSet<Integer>();
		for (String item : items) {
			int item_id = Integer.parseInt(item);
			sorted_items.add(item_id);
		}

		this.items = new ArrayList<String>();
		for (Integer sorted_item : sorted_items) {
			this.items.add(sorted_item+"");
		}


        this.distribution = distribution.getNewInstance();
        this.clientID = clientID;
        this.private_id = personal_number;


        this.progressBar = progressBar;
        rand = new Random();
        items_to_buy = new ArrayDeque<String>();

        generateItems();

    }


    private void generateItems() {
        watching_item = false;
        cart++;
        int item_num = rand.nextInt(9) + 1;

   //   	item_num = 5;

        for (int num = 0; num < item_num; num++) {

            int item_index = distribution.getNextElement();
            String item = items.get(item_index);
            items_to_buy.add(item);
        }

    }

    public Operation getNextOperation() {


        progressBar.increment(private_id);
        Operation op = null;
        if (items_to_buy.isEmpty() && watching_item == false) {

            Map<String, Object> parameters = new TreeMap<String, Object>();
            parameters.put("CART_ID", clientID + "." + cart);
            op = new Operation("BUY_CART", parameters);
            generateItems();

            if (debug_number > 9) {
                System.out.println("[WARN|" + private_id + ":] SUSPECT NUMBER OF ITEMS BEIng ADDED TO CART");
            }
            debug_number = 0;
        } else {
            //Fetch the product stock, to buy it
            if (watching_item) {
                watching_item = false;
                int stock = -1;
                if (last_stock.getResult() == null) {
                    System.out.println("[ERROR:] ITEM BEING WATCHED WAS NULL STOCK");
                    op = new Operation("", null);

                    return op;
                } else {
                    int value = (Integer) last_stock.getResult();
                    stock = (int) value;
                }

                if (stock <= 0) {

                    if (stock < 0)
                        System.out.println("[ERROR:] CANT RETRIEVE STOCK FOR ITEM : " + last_item);
                    op = new Operation("INVALID_STOCK", null);
                    return op;
                } else if (stock > 0) {
                    int limit = (stock > 5) ? 5 : stock;
                    int toAdd = rand.nextInt(limit) + 1;
                    Map<String, Object> parameters = new TreeMap<String, Object>();
                    parameters.put("ITEM_ID", last_item);
                    parameters.put("CART_ID", clientID + "." + cart);
                    parameters.put("QTY", toAdd);
                    op = new Operation("ADD_TO_CART", parameters);
                }
            }
            //See the product stock
            else {
                debug_number++;
                String item = items_to_buy.poll();
                Map<String, Object> parameters = new TreeMap<String, Object>();
                parameters.put("ITEM_ID", item);
                last_item = item;
                op = new Operation("GET_ITEM_STOCK", parameters);
                last_stock = op;
                watching_item = true;
            }
        }

        return op;
    }
}
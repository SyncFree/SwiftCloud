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

package org.uminho.gsd.benchmarks.helpers;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;


/**Test class*/
public class Workload {

    public static void main(String[] args) {


        //values read form file or hardcoded
        Map<String, Double> tpcw_mix = new TreeMap<String, Double>();
        tpcw_mix.put("home", 29.00d);
        tpcw_mix.put("new_products", 11.00d);
        tpcw_mix.put("best_sellers", 11.00d);
        tpcw_mix.put("product_detail", 21.00d);
        tpcw_mix.put("search", 23.00d);    //Request + confirm
        tpcw_mix.put("register/login", 0.82d);
        tpcw_mix.put("order_inquiry", 0.55d);  //Inquiry+ display
        tpcw_mix.put("admin_change", 0.19d);  //Request + confirm
        tpcw_mix.put("shoppingCart", 2.00d);
        tpcw_mix.put("buy_request", 0.75d);
        tpcw_mix.put("buy_confirm", 0.69d);


        Map<String, Double> workload_values = new TreeMap<String, Double>();
        double buy_request_prob = 1;
        double buy_confirm_prob = 1;

        double remaining = 0;

        //Aggregate probabilities for the generation cycle.
        //As the the values for the buy confirm and buy request are not included, there is a second iteration to add the remaining value to the other options.
        for (int i = 0; i < 2; i++) {
            workload_values.clear();

            double aggregated_probability = 0;
            double prob = tpcw_mix.get("home");
            workload_values.put("home", prob + (remaining * (prob / 100d)));
            aggregated_probability = workload_values.get("home");

            prob = tpcw_mix.get("new_products");
            workload_values.put("new_products", prob + aggregated_probability + (remaining * (prob / 100d)));
            aggregated_probability += prob + (remaining * (prob / 100d));

            prob = tpcw_mix.get("best_sellers");
            workload_values.put("best_sellers", prob + aggregated_probability + (remaining * (prob / 100d)));
            aggregated_probability += prob + (remaining * (prob / 100d));

            prob = tpcw_mix.get("product_detail");
            workload_values.put("product_detail", prob + aggregated_probability + (remaining * (prob / 100d)));
            aggregated_probability += prob + (remaining * (prob / 100d));

            prob = tpcw_mix.get("search");
            workload_values.put("search", prob + aggregated_probability + (remaining * (prob / 100d)));
            aggregated_probability += prob + (remaining * (prob / 100d));

            prob = tpcw_mix.get("register/login");
            workload_values.put("register/login", prob + aggregated_probability + (remaining * (prob / 100d)));
            aggregated_probability += prob + (remaining * (prob / 100d));

            prob = tpcw_mix.get("order_inquiry");
            workload_values.put("order_inquiry", prob + aggregated_probability + (remaining * (prob / 100d)));
            aggregated_probability += prob + (remaining * (prob / 100d));

            prob = tpcw_mix.get("admin_change");
            workload_values.put("admin_change", prob + aggregated_probability + (remaining * (prob / 100d)));
            aggregated_probability += prob + (remaining * (prob / 100d));

            prob = tpcw_mix.get("shoppingCart");
            workload_values.put("shoppingCart", prob + aggregated_probability + (remaining * (prob / 100d)));
            aggregated_probability += prob + (remaining * (prob / 100d));

            remaining = 100 - aggregated_probability;
            System.out.println("A" + i + "" + (100 - aggregated_probability));
        }

        //set buy options probabilities. If shopping cart is 2% and the buy request is 0,75% then of each cart there is a 0,41% probability.
        double sc_prob = tpcw_mix.get("shoppingCart");
        double br_prob = tpcw_mix.get("buy_request");
        buy_request_prob = br_prob / sc_prob;
        double bc_prob = tpcw_mix.get("buy_confirm");
        buy_confirm_prob = bc_prob / br_prob;

        System.out.println("BR:" + buy_request_prob + "  BC:" + buy_confirm_prob);

        Random rand = new Random();

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

        int num_operations = 1000000;
        boolean shopping = false;
        boolean confirm = false;
        for (int i = 0; i < num_operations; i++) {


            if (shopping) {

                //if confirm
                if (confirm) {
                    i11++;
                    //execute buy confirm operation
                    shopping = false;
                    confirm = false;
                } else {
                    i10++;
                    double d = rand.nextDouble();
                    if (d < buy_confirm_prob) {
                        //on next iteration do a buy confirm
                        confirm = true;
                    } else {
                        shopping = false;
                        confirm = false;
                    }
                    //execute buy request operation
                }
            } else {
                double d = rand.nextDouble() * 100;
                if (d < workload_values.get("home")) {
                    i1++;
                    //execute home operation

                } else if (d < workload_values.get("new_products")) {
                    i2++;
                    //execute new products operation

                } else if (d < workload_values.get("best_sellers")) {
                    i3++;
                    //execute best sellers operation

                } else if (d < workload_values.get("product_detail")) {
                    i4++;
                    //execute product detail operation


                } else if (d < workload_values.get("search")) {
                    i5++;
                    //execute product search operation


                } else if (d < workload_values.get("register/login")) {
                    i6++;
                    double decision_factor = rand.nextDouble();

                    if (decision_factor < 0.2) {
                        //register customer
                    } else {
                        //do login
                    }

                } else if (d < workload_values.get("order_inquiry")) {
                    i7++;
                    //execute order operation


                } else if (d < workload_values.get("admin_change")) {
                    i8++;
                    //execute admin operation

                } else if (d < workload_values.get("shoppingCart")) {
                    i9++;
                    double d2 = rand.nextDouble();
                    if (d2 < buy_request_prob) {
                        //on next operation do a buy request id the probability checks
                        shopping = true;
                        confirm = false;
                    } else {
                        shopping = false;
                        confirm = false;
                    }

                }

            }

        }

        int operation_num = num_operations;
        System.out.println("-------");
        System.out.println("OP1 HOME:" + ((i1 / (operation_num * 1d)) * 100));
        System.out.println("OP2 NewProd:" + ((i2 / (operation_num * 1d)) * 100));
        System.out.println("OP3 BestSellers:" + ((i3 / (operation_num * 1d)) * 100));
        System.out.println("OP4 PDetail:" + ((i4 / (operation_num * 1d)) * 100));
        System.out.println("OP5 Search:" + ((i5 / (operation_num * 1d)) * 100));
        System.out.println("OP9 ShoppingCart:" + ((i9 / (operation_num * 1d)) * 100));
        System.out.println("OP6 CostumerRegis:" + ((i6 / (operation_num * 1d)) * 100));
        System.out.println("OP10 BRequest:" + ((i10 / (operation_num * 1d)) * 100));
        System.out.println("OP11 BConfirm:" + ((i11 / (operation_num * 1d)) * 100));
        System.out.println("OP7 OrderInqui:" + ((i7 / (operation_num * 1d)) * 100));
        System.out.println("OP8 AdminCofirm:" + ((i8 / (operation_num * 1d)) * 100));

        System.out.println("-------");
    }
}

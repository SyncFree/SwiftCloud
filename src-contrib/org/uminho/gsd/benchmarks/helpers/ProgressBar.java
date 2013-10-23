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

import java.util.concurrent.atomic.AtomicInteger;


public class ProgressBar {

    AtomicInteger[] progress;
    int top_limit;
    boolean[] terminated;
    int terminated_count;
    int num_elements;
    boolean perc_term = true;

    public ProgressBar(int numberElements, int limit) {
        progress = new AtomicInteger[numberElements];
        terminated = new boolean[numberElements];
        for (int i = 0; i < numberElements; i++) {
            progress[i] = new AtomicInteger(0);
            terminated[i] = false;
        }
        top_limit = limit;
        num_elements = numberElements;


        terminated_count = 0;

    }

    public void setProgress(int index, int progress) {
        this.progress[index].set(progress);
    }

    public void addProgress(int index, int progress) {
        this.progress[index].addAndGet(progress);
    }

    public void increment(int index) {
        this.progress[index].incrementAndGet();
    }

    public void printProcess(final long delays) {

        Runnable run = new Runnable() {

            public void run() {
                boolean terminate = false;
                long starting_time = System.currentTimeMillis();

                while (!terminate) {

                    int total = 0;

                    for (int i = 0; i < num_elements; i++) {
                        int pg = progress[i].get();
                        if (!terminated[i] && pg == top_limit) {
                            terminated[i] = true;
                            terminated_count++;
                        }
                        total += pg;
                    }
                    int vintage = (total * 20) / (top_limit * num_elements);
                    int percentage = (total * 100) / (top_limit * num_elements);
                    int rest = 20 - vintage;

                    System.out.print("\r");
                    System.out.print("||");
                    for (int pc = 0; pc < vintage; pc++) {
                        System.out.print("=");
                    }
                    for (int pc = 0; pc < rest; pc++) {
                        System.out.print(".");
                    }
                    System.out.print("||");
                    if (perc_term) {
                        System.out.print("[" + percentage + "%]    ");
                    } else {
                        System.out.print("(" + terminated_count + "/" + num_elements + ") ");
                    }

                    long elapsed_time = System.currentTimeMillis() - starting_time;
                    long remaining_operations = (top_limit * num_elements) - total;
                    if (elapsed_time > 1000 || total != 0) {

                        double operations_per_second = (total * 1d) / ((elapsed_time * 1d) / 1000d);
                        int remaining_time_in_seconds = (int) (remaining_operations / operations_per_second);
                        int minutes_remaining = remaining_time_in_seconds / 60;
                        int seconds_remaining = remaining_time_in_seconds - (minutes_remaining * 60);

                        int minutes_passed = (int) (elapsed_time / 60000);
                        int seconds_passed = (int) (((elapsed_time) - minutes_passed * 60000) / 1000);
                        int hour_passed = 0;
                        int hour_remaining = 0;
                        if (minutes_passed > 59 || minutes_remaining > 59) {

                            if (minutes_passed > 59) {
                                hour_passed = minutes_passed / 60;
                                minutes_passed = minutes_passed - hour_passed * 60;
                            }
                            if (minutes_remaining > 59) {
                                hour_remaining = minutes_remaining / 60;
                                minutes_remaining = minutes_remaining - hour_remaining * 60;
                            }
                        }
                        String seconds_p =(seconds_passed<10)? "0"+seconds_passed : seconds_passed+"";
                        String minutes_p =(minutes_passed<10)? "0"+minutes_passed : minutes_passed+"";
                        String seconds_r =(seconds_remaining<10)? "0"+seconds_remaining : seconds_remaining+"";
                        String minutes_r =(minutes_remaining<10)? "0"+minutes_remaining : minutes_remaining+"";


                        System.out.print("| T: " + hour_passed + " : " + minutes_p + " : " + seconds_p + " / R: " + hour_remaining + " : " + minutes_r + " : " + seconds_r + " |   ");

                    } else {
                        System.out.print("| T: - : -- : -- / R: - : -- : -- |         ");
                    }


                    perc_term = !perc_term;


                    try {
                        Thread.sleep(delays);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                    if (num_elements == terminated_count) {
                        terminate = true;
                        System.out.println();
                    }

                }
            }
        };
        Thread t = new Thread(run,"Progress Bar");
        t.start();
    }

    public int getTop_limit() {
        return top_limit;
    }

    public static void main(String[] args) {
        ProgressBar bar = new ProgressBar(2, 10);
        bar.printProcess(10);

        Runnable run = new Runnable() {
            ProgressBar pbar;

            public Runnable setBar(ProgressBar bar) {
                pbar = bar;
                return this;
            }

            public void run() {
                for (int i = 0; i < 10; i++) {
                    pbar.increment(0);
                    try {
                        Thread.sleep(15);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
            }
        }.setBar(bar);

        Runnable run2 = new Runnable() {
            ProgressBar pbar;

            public Runnable setBar(ProgressBar bar) {
                pbar = bar;
                return this;
            }

            public void run() {
                for (int i = 0; i < 10; i++) {
                    pbar.increment(1);
                    try {
                        Thread.sleep(15);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }

            }
        }.setBar(bar);

        Thread t1 = new Thread(run);
        Thread t2 = new Thread(run2);
        t1.start();
        t2.start();


    }


}

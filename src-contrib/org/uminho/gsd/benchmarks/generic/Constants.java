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

package org.uminho.gsd.benchmarks.generic;


public class Constants {

    public static /* final */ int NUM_EBS = 10;
    public static /* final */ int NUM_ITEMS = 10000;
    public static /* final */ int NUM_CUSTOMERS = NUM_EBS * 2880;
    public static /* final */ int NUM_ADDRESSES = 2 * NUM_CUSTOMERS;
    public static /* final */ int NUM_AUTHORS = (int) (.25 * NUM_ITEMS);
    public static /* final */ int NUM_ORDERS = (int) (.9 * NUM_CUSTOMERS);
    public static /* final */ int NUM_COUNTRIES = 92; // this is constant. Never changes!


}

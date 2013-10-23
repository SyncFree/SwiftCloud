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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.uminho.gsd.benchmarks.helpers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BenchmarkUtil {


    private static Random rand = new Random();

    private static final String[] digS = {
            "BA", "OG", "AL", "RI", "RE", "SE", "AT", "UL", "IN", "NG"
    };

    public static GregorianCalendar getRandomDate(int firstYar, int lastYear) {
        int month, day, year, maxday;

        year = getRandomInt(firstYar, lastYear);
        month = getRandomInt(0, 11);

        maxday = 31;
        if (month == 3 | month == 5 | month == 8 | month == 10) {
            maxday = 30;
        } else if (month == 1) {
            maxday = 28;
        }

        day = getRandomInt(1, maxday);
        return new GregorianCalendar(year, month, day);
    }

    public static int getRandomInt(int lower, int upper) {

        int num = (int) Math.floor(rand.nextDouble() * ((upper + 1) - lower));
        if (num + lower > upper || num + lower < lower) {
            System.out.println("ERROR: Random returned value of of range!");
            System.exit(1);
        }
        return num + lower;
    }

    public static int getRandomNString(int num_digits) {
        int return_num = 0;
        for (int i = 0; i < num_digits; i++) {
            return_num += getRandomInt(0, 9)
                    * (int) java.lang.Math.pow(10.0, (double) i);
        }
        return return_num;
    }

    public static String getRandomAString(int min, int max) {
        String newstring = new String();
        int i;
        final char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
                'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
                'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
                'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#',
                '$', '%', '^', '&', '*', '(', ')', '_', '-', '=', '+',
                '{', '}', '[', ']', '|', ':', ';', ',', '.', '?', '/',
                '~'}; //78 characters
        int strlen = (int) Math.floor(rand.nextDouble() * ((max - min) + 1));
        if (strlen < 1) strlen = min;
        strlen += min;
        for (i = 0; i < strlen; i++) {
            char c = chars[(int) Math.floor(rand.nextDouble() * 78)];
            newstring = newstring.concat(String.valueOf(c));
        }
        return newstring;
    }


    public static byte[] getBytes(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            oos.close();
            bos.close();
            byte[] data = bos.toByteArray();
            return data;
        } catch (IOException ex) {
            Logger.getLogger(BenchmarkUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static Object toObject(byte[] bytes) {
        try {
            Object object = null;
            object = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes)).readObject();
            return object;
        } catch (IOException ex) {
            Logger.getLogger(BenchmarkUtil.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BenchmarkUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static String getRandomAString(int length) {
        String newstring = new String();
        int i;
        final char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
                'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
                'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
                'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#',
                '$', '%', '^', '&', '*', '(', ')', '_', '-', '=', '+',
                '{', '}', '[', ']', '|', ':', ';', ',', '.', '?', '/',
                '~', ' '}; //79 characters
        for (i = 0; i < length; i++) {
            char c = chars[(int) Math.floor(rand.nextDouble() * 79)];
            newstring = newstring.concat(String.valueOf(c));
        }
        return newstring;
    }


    public static TreeMap<String, Integer> randomizeMap(Map<String, Integer> map) {

        Random r = new Random();
        TreeMap<String, Integer> new_map = new TreeMap<String, Integer>();
        for (String m : map.keySet()) {
            new_map.put(r.nextInt(10) + ":" + m, map.get(m));

        }
        return new_map;
    }

    public static String DigSyl(int d, int n) {
        String s = "";

        if (n == 0) return (DigSyl(d));
        for (; n > 0; n--) {
            int c = d % 10;
            s = digS[c] + s;
            d = d / 10;
        }

        return (s);
    }

    public static String DigSyl(int d) {
        String s = "";

        for (; d != 0; d = d / 10) {
            int c = d % 10;
            s = digS[c] + s;
        }

        return (s);
    }


    class MethodExecuter implements Runnable {

        Object invoked;
        Method m;
        Object[] parameters;

        MethodExecuter(Object invoked, Method m, Object[] parameters) {
            this.invoked = invoked;
            this.m = m;
            this.parameters = parameters;
        }

        public void run() {
            try {
                m.invoke(invoked, parameters);
            } catch (IllegalAccessException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (InvocationTargetException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

}

class StringComparator implements Comparator {

    public int compare(Object o1, Object o2) {

        Random r = new Random();
        if (!(o1 instanceof String) || !(o2 instanceof String))
            return 0;


        return r.nextInt(2) - 1;

    }
}
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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

public class SqlReader {


    public static Map<String, String> parse(String fileName) {


        Map<String, String> queryMap = new TreeMap<String, String>();
        try {
            // Open the file that is the first
            // command line parameter
            FileInputStream fstream = new FileInputStream(fileName);
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            //Read File Line By Line
            String query_name = "";
            String query = "";
            while ((strLine = br.readLine()) != null) {

                if (strLine.trim().equals("") || strLine.trim().startsWith("#")) {
                    continue;
                } else if (strLine.startsWith("@")) {

                    if (!query_name.equals("") && !query.equals("")) {
                        queryMap.put(query_name, query);
                    }

                    query_name = strLine.split("=")[0];
                    query = strLine.replace(query_name + "=", "");
                } else {
                    query = query + strLine;
                }


            }
            //Close the input stream
            in.close();
        } catch (Exception e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }

        return queryMap;

    }

    public static void main(String[] args) {
        Map<String, String> s = SqlReader.parse("conf/DataStore/sql-mysql.properties");
        for (String q : s.keySet()) {
            System.out.println(q + "=" + s.get(q));

        }
    }

}

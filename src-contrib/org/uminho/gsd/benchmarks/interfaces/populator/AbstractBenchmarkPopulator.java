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

package org.uminho.gsd.benchmarks.interfaces.populator;


import org.uminho.gsd.benchmarks.helpers.JsonUtil;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;

import java.io.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractBenchmarkPopulator {


    //The configuration file
    private String conf_filename;
    /**
     * Configuration map*
     */
    protected Map<String, Map<String, String>> configuration;
    /**
     * Database interface*
     */
    protected AbstractDatabaseExecutorFactory database_interface_factory;

    protected AbstractBenchmarkPopulator(AbstractDatabaseExecutorFactory database_interface_factory, String conf_filename) {
        this.conf_filename = conf_filename;
        this.database_interface_factory = database_interface_factory;
        loadFile();
    }

    /**
     * @return true if success, false otherwise
     */
    public abstract boolean populate();

    public abstract void cleanDB() throws Exception;

    public abstract void BenchmarkClean() throws Exception;

    public void loadFile() {


        FileInputStream in = null;
        String jsonString_r = "";

        if (!conf_filename.endsWith(".json")) {
            conf_filename = conf_filename + ".json";
        }

        try {
            in = new FileInputStream("conf/Populator/" + conf_filename);
            BufferedReader bin = new BufferedReader(new InputStreamReader(in));
            String s = "";
            StringBuilder sb = new StringBuilder();
            while (s != null) {
                sb.append(s);
                s = bin.readLine();
            }
            jsonString_r = sb.toString().replace("\n", "");
            bin.close();
            in.close();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(AbstractDatabaseExecutorFactory.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AbstractDatabaseExecutorFactory.class.getName()).log(Level.SEVERE, null, ex);

        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                Logger.getLogger(AbstractDatabaseExecutorFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        Map<String, Map<String, String>> map = JsonUtil.getStringMapMapFromJsonString(jsonString_r);
        configuration = map;
    }
}

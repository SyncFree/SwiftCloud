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

package org.uminho.gsd.benchmarks.interfaces.executor;


import org.uminho.gsd.benchmarks.benchmark.BenchmarkExecutor;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkNodeID;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.helpers.JsonUtil;
import org.uminho.gsd.benchmarks.helpers.TPM_counter;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client Factory<br>
 * Configuration and creation of database clients is handle by this class.
 *
 * @see org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface
 */
public abstract class AbstractDatabaseExecutorFactory {

	/**
	 * Information collected from the configuration file *
	 */
	protected Map<String, Map<String, String>> conf;
	//Configuration file name
	private String dataStore_file_name;
	/**
	 * BenchmarkExecutor*
	 */
	protected BenchmarkExecutor executor;
	/**
	 * Benchmarking node id*
	 */
	protected BenchmarkNodeID nodeID;
	/**
	 * The number of clients in one node*
	 */
	protected int client_number;


	protected List<TPM_counter> tpm_counters = new LinkedList<TPM_counter>();


	private Timer data_extraction = new Timer("Statistics calculation", true);

	private ResultHandler stats_handler;

	private boolean do_tmp_counting = false;


	protected AbstractDatabaseExecutorFactory(BenchmarkExecutor executor, String conf_file) {

		this.dataStore_file_name = conf_file;
		this.executor = executor;
		loadFile();
	}

	/**
	 * Method that loads workload info.
	 */
	private void loadFile() {

		FileInputStream in = null;
		String jsonString_r = "";

		if (!dataStore_file_name.endsWith(".json")) {
			dataStore_file_name = dataStore_file_name + ".json";
		}

		try {
			in = new FileInputStream("conf/DataStore/" + dataStore_file_name);
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
		conf = map;
	}

	public abstract DatabaseExecutorInterface getDatabaseClient();


	public void startStats() {
		data_extraction.schedule(tpm_calculation, 60000, 60000);
	}

	public void setNodeId(BenchmarkNodeID id) {
		nodeID = id;
	}

	/**
	 * Sets the number of used clients in one node.
	 */
	public void setClientNumber(int client_number) {
		this.client_number = client_number;

	}


	public void setStats_handler(ResultHandler stats_handler) {
		this.stats_handler = stats_handler;
	}

	TimerTask tpm_calculation = new TimerTask() {

		public void run() {
			int tpm = 0;
			if (do_tmp_counting) {
				for (TPM_counter tpm_counter : tpm_counters) {
					tpm += tpm_counter.get_and_reset();
				}
			    if(stats_handler!=null){
					stats_handler.logResult("TPM", (tpm));
					System.out.println("TPM:" + tpm);
				}
			}

		}
	};


	public synchronized void registerCounter(TPM_counter tpm_counter) {
		tpm_counters.add(tpm_counter);
	}


	public void initTPMCounting() {
		do_tmp_counting = true;
	}

}

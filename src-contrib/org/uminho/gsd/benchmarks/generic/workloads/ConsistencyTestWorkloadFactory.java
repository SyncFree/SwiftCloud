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


import org.uminho.gsd.benchmarks.benchmark.BenchmarkExecutor;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkMain;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkSlave;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.generic.Constants;
import org.uminho.gsd.benchmarks.generic.entities.Results;
import org.uminho.gsd.benchmarks.helpers.ProgressBar;
import org.uminho.gsd.benchmarks.interfaces.ProbabilityDistribution;
import org.uminho.gsd.benchmarks.interfaces.Workload.AbstractWorkloadGeneratorFactory;
import org.uminho.gsd.benchmarks.interfaces.Workload.Operation;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Workload that generates operations to generate possible inconsistency in the database due to the lack of ACID transactions.
 */
public class ConsistencyTestWorkloadFactory extends AbstractWorkloadGeneratorFactory {

	//The items and their stock in the database
	Map<String, Integer> items;

	//The item ids
	ArrayList<String> items_ids;

	//The probability distribution
	ProbabilityDistribution distribution;

	//The number of clients
	int client_number;

	/**
	 * Initial Stock
	 */
	int initial_stock;


	public static int mean_cart_items = 5;


	/**
	 * ResultHandler*
	 */
	ResultHandler globalResultHandler;

	/**
	 * The path to store the result file*
	 */
	String result_path = "";


	ProgressBar progressBar;

	/**
	 * The generic construct that invokes the init() method.
	 *
	 * @param workload The workload file name to be fetched from the configuration files, or other place
	 */
	public ConsistencyTestWorkloadFactory(BenchmarkExecutor executor, String workload) {
		super(executor, workload);


		//loading the selected distribution from file.
		String distributionClass;
		Map<String, String> probabilityDistributionInfo;
		probabilityDistributionInfo = new TreeMap<String, String>();

		if (!info.containsKey("ProbabilityDistributions")) {
			System.out.println("[WARNING:] NO DISTRIBUTION INFO FOUND USING NORMAL DISTRIBUTION");
			distributionClass = "benchmarks.interfaces.ProbabilityDistribution.ZipfDistribution";
			probabilityDistributionInfo.put("skew", "1");
		} else {
			Map<String, String> distribution_info = info.get("ProbabilityDistributions");
			if (!distribution_info.containsKey("Distribution")) {
				distributionClass = "benchmarks.interfaces.ProbabilityDistribution.ZipfDistribution";
				System.out.println("[WARNING:] NO DISTRIBUTION INFO FOUND USING NORMAL DISTRIBUTION");
				probabilityDistributionInfo.put("skew", "1");
			} else {
				distributionClass = distribution_info.get("Distribution");
				for (String s : distribution_info.keySet()) {
					probabilityDistributionInfo.put(s, distribution_info.get(s));
				}
			}
		}

		if (!info.containsKey("Configuration")) {
			System.out.println("[WARNING:] NO TPCW CONFIGURATION DATA");

		} else {
			Map<String, String> conf = info.get("Configuration");


			if (!conf.containsKey("initialStock")) {
				System.out.println("[WARNING:] NO STOCK HANDLER CONFIGURATION DATA -> initialStock at 50 000 ");
				initial_stock = 50000;
			} else {
				initial_stock = Integer.parseInt(conf.get("initialStock"));
			}

			if (!conf.containsKey("resultPath")) {
				System.out.println("[INFO:] Default result path: /result ");
				result_path = "./Results";
			} else {
				result_path = conf.get("resultPath");
			}

			if (!conf.containsKey("name")) {
				System.out.println("[INFO:] Default name : TPCW_WORKLOAD");
				workloadName = "TPCW_WORKLOAD";
			} else {
				workloadName = conf.get("name");
			}

			if (!conf.containsKey("mean_cart_items")) {
				System.out.println("[INFO]: Mean cart items = 5");
				mean_cart_items = 5;
			} else {
				mean_cart_items = Integer.parseInt(conf.get("mean_cart_items").trim());
			}


		}

//        if (!info.containsKey("StockHandler")) {
//            System.out.println("[WARNING:] NO STOCK HANDLER CONFIGURATION DATA -> 10 crawlers units will be deploy with outOfStock at 0 units and restock to 10 units");
//            out_of_stock = 0;
//            restock = 10;
//        } else {
//            Map<String, String> conf = info.get("StockHandler");
//
//            if (!conf.containsKey("initialStock")) {
//                System.out.println("[WARNING:] NO STOCK HANDLER CONFIGURATION DATA -> initialStock at 50 000 ");
//                initial_stock = 50000;
//            } else {
//                initial_stock = Integer.parseInt(conf.get("initialStock"));
//            }
//
//            if (!conf.containsKey("outOfStock")) {
//                System.out.println("[WARNING:] NO STOCK HANDLER CONFIGURATION DATA -> outOfStock at 0 ");
//                out_of_stock = 0;
//            } else {
//                out_of_stock = Integer.parseInt(conf.get("outOfStock"));
//            }
//
//            if (!conf.containsKey("restock")) {
//                System.out.println("[WARNING:] NO STOCK HANDLER CONFIGURATION DATA -> restock to 10 units ");
//                restock = 10;
//            } else {
//                restock = Integer.parseInt(conf.get("restock"));
//            }
//
//            if (!conf.containsKey("numCrawlers")) {
//                System.out.println("[WARNING:] NO STOCK HANDLER CONFIGURATION DATA ->  10 units crawlers units deployed ");
//                stock_crawlers = 10;
//            } else {
//                stock_crawlers = Integer.parseInt(conf.get("numCrawlers"));
//            }
//        }


		try {
			distribution = (ProbabilityDistribution) Class.forName(distributionClass).getConstructor().newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		} catch (IllegalAccessException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		} catch (InvocationTargetException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		} catch (NoSuchMethodException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		} catch (ClassNotFoundException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
		distribution.setInfo(probabilityDistributionInfo);

	}


	@Override
	public void init() throws Exception {

		items = new TreeMap<String, Integer>();
		items_ids = new ArrayList<String>();


		Map<String, Object> param = new TreeMap<String, Object>();

		param.put("STOCK", initial_stock);
		Operation op = new Operation("Get_Stock_And_Products_after_increment", param);

		DatabaseExecutorInterface databaseClient = this.databaseFactory.getDatabaseClient();
		try {
			databaseClient.execute(op);
		} catch (NoSuchFieldException e) {
			System.out.println("[ERROR:] THIS OPERATION DOES NOT EXiST : " + e.getMessage());
		}

		Map<String, Map<String, Object>> items_info = (Map<String, Map<String, Object>>) op.getResult();


		System.out.println("[INFO:] ITEMS COLLECTED FROM THE DATABASE: SIZE = " + items_info.size());
		if (items_info.isEmpty()) {
			System.out.println("[INFO:] NO ITEMS  => BENCHMARK END, GOODBYE SIR!");
			System.exit(0);
		}

		items = new TreeMap<String, Integer>();
		for (String ik : items_info.keySet()) {

			ArrayList<Object> data = new ArrayList<Object>();

			Object stock_obj = items_info.get(ik).get("I_STOCK");
			if (stock_obj == null) {
				stock_obj = items_info.get(ik).get("i_stock");
			}


			int stock = 0;
			stock = (Integer) stock_obj;

			//if (stock != 500000) {
		//		System.out.println("Error: wrong initial stock");
		//	}

			items.put(ik, (int) stock);
			items_ids.add(ik);
		}

		Constants.NUM_ITEMS = items_info.size();


		distribution.init(items_info.size(), null);
		progressBar = new ProgressBar(executor.num_clients, executor.num_operations);
		globalResultHandler = new ResultHandler(workloadName, -1);

		Map<String, String> info = new LinkedHashMap<String, String>();

		info.put("Workload class:", TPCWWorkloadFactory.class.getName());
		info.put("Workload conf:", workloadName);
		info.put("Workload values:", "NA");
		info.put("Database Executor", databaseFactory.getClass().getName());
		info.put("Database engine conf:", databaseClient.getInfo().toString());
		info.put("----", "----");
		String think_time = (BenchmarkMain.thinkTime == -1) ? "tpcw think time" : "user set: " + BenchmarkMain.thinkTime;
		info.put("Think time", think_time);
		info.put("Client num", executor.num_clients + "");
		info.put("Operation num", executor.num_operations + "");
		info.put("Distribution", distribution.getName());
		info.put("Distribution conf", distribution.getInfo().toString());
		info.put("----", "----");

		GregorianCalendar date = new GregorianCalendar();
		String data_string = date.get(GregorianCalendar.YEAR) + "\\" + (date.get(GregorianCalendar.MONTH) + 1) + "\\" + date.get(GregorianCalendar.DAY_OF_MONTH) + " -- " + date.get(GregorianCalendar.HOUR_OF_DAY) + ":" + date.get(GregorianCalendar.MINUTE) + "";
		info.put("Start", data_string);

		globalResultHandler.setBechmark_info(info);

	}

	@Override
	public WorkloadGeneratorInterface getClient() {

		if (client_number == 0) {
			progressBar.printProcess(1500);
		}

		ConsistencyWorkloadGenerator client = new ConsistencyWorkloadGenerator(items_ids, distribution, nodeID.getId() + "." + client_number, client_number, progressBar);
		client_number++;
		return client;

	}

	@Override
	public void finishExecution(List<ResultHandler> collected_results) {


		Map<String, Integer> BoughtItems = new TreeMap<String, Integer>();

		int bought_qty = 0;
		int buying_actions = 0;
		int bought_carts = 0;
		int zeros = 0;


		for (ResultHandler client : collected_results) {
			globalResultHandler.addResults(client);

			HashMap<String, Object> map = client.getResulSet();
			if (!map.isEmpty()) {

				Map<String, Integer> partialBought = (Map<String, Integer>) client.getResulSet().get("bought");
				bought_qty += (Integer) client.getResulSet().get("total_bought");
				buying_actions += (Integer) client.getResulSet().get("buying_actions");
				bought_carts += (Integer) client.getResulSet().get("bought_carts");
				zeros += (Integer) client.getResulSet().get("zeros");


				for (String item : partialBought.keySet()) {
					if (BoughtItems.containsKey(item)) {
						int bought = partialBought.get(item);
						int tbought = BoughtItems.get(item);
						BoughtItems.put(item, bought + tbought);
					} else {
						int bought = partialBought.get(item);
						BoughtItems.put(item, bought);
					}
				}
			}
		}


		//   System.out.println("Collected results sizes:" +BoughtItems.size());

		try {
			Thread.sleep(1500);
		} catch (InterruptedException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}

//		//debug-->
//
//
//		for (String item_string_id : BoughtItems.keySet()) {
//			int item_id = Integer.parseInt(item_string_id);
//			if(item_id<100){
//				System.out.println(item_id+" - "+BoughtItems.get(item_string_id));
//			}
//
//
//		}
//
//		//-->debug

		System.out.println("[INFO:] TOTAL BOUGHT: " + bought_qty);
		System.out.println("[INFO:] BUYING ACTIONS: " + buying_actions);
		System.out.println("[INFO:] BOUGHT CARTS: " + bought_carts);
//		System.out.println("[INFO:] ZERO STOCK SELLS: " + zeros);
		System.out.println("[INFO:] WRITING RESULTS TO THE DATABASE");

		DatabaseExecutorInterface database_client = this.databaseFactory.getDatabaseClient();

		if (nodeID.isMaster()) {

			//     System.out.println("ITEM IDS SIZE: " + items_ids.size());

			for (String item : items_ids) {
				int bought = BoughtItems.containsKey(item) ? BoughtItems.get(item) : 0;
				Results result = new Results(bought, items.get(item), nodeID.getId() + "");
				try {
					database_client.insert(item, "results", result);
				} catch (Exception e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}
			}
		} else {
			for (String item : items_ids) {
				int bought = BoughtItems.containsKey(item) ? BoughtItems.get(item) : 0;
				Results result = new Results(bought, 0, nodeID.getId() + "");
				try {
					database_client.insert(item, "results", result);
				} catch (Exception e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}
			}
		}

		database_client.closeClient();
		System.out.println("[INFO:] EXECUTION FINISHED");
	}

	@Override
	public void consolidate() throws Exception {

		System.out.println("[INFO:] ANALYZING RESULTS");

		if (nodeID.isMaster()) { //Master

			Map<String, Integer> added_stock = new TreeMap<String, Integer>();//stockHandler.finishHandler();

			System.out.println("[INFO:] READING RESULTS");

			Map<String, int[]> items_FinalInfo = new TreeMap<String, int[]>();


			DatabaseExecutorInterface databaseCrudClient = databaseFactory.getDatabaseClient();
			Operation op = new Operation("GET_BENCHMARK_RESULTS", null);
			try {
				databaseCrudClient.execute(op);
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			}
			Map<String, Map<String, Map<String, Object>>> result_info = (Map<String, Map<String, Map<String, Object>>>) op.getResult();

			int items_with_no_added_stock = 0;

			int total_out_stock = 0;

			for (String clientID : result_info.keySet()) {

				//for each client
				// System.out.println("Clients:" + result_info.keySet().size());
				Map<String, Map<String, Object>> item_info = result_info.get(clientID);
				//       System.out.println("[INFO:] Items result size:" + item_info.keySet().size());
				for (String itemID : item_info.keySet()) {
					Map<String, Object> item_data = item_info.get(itemID);
					if (!items_FinalInfo.containsKey(itemID)) {

						Object o = databaseCrudClient.read(itemID, "item", "I_STOCK", null);
						int stock = 0;
						if (o != null) {
							stock = (Integer) o;
						} else {
							System.out.println("[ERROR:]ITEM CURRENT STOCK NOT FOUND");
						}

						if (added_stock.containsKey(itemID)) {
							items_FinalInfo.put(itemID, new int[]{added_stock.get(itemID), 0, (int) stock});
						} else {
							items_with_no_added_stock++;
							items_FinalInfo.put(itemID, new int[]{0, 0, (int) stock});
						}

					}

					//item info
					//[0] initial + added by the stock handler
					//[1] bought
					//[2] still in the database

					for (String value_name : item_data.keySet()) {
						if (value_name.equalsIgnoreCase("STOCK")) {
							int current_s = items_FinalInfo.get(itemID)[0];
							int read_s = (Integer) item_data.get(value_name);

							items_FinalInfo.get(itemID)[0] = current_s + read_s;
						} else if (value_name.equalsIgnoreCase("BOUGHT")) {


							int current_b = items_FinalInfo.get(itemID)[1];
							int read_b = (Integer) item_data.get(value_name);

							if (items_FinalInfo.get(itemID)[2] == 500000 && (current_b + read_b) != 0) {
								System.out.println("item X:" + itemID);
							}

							items_FinalInfo.get(itemID)[1] = current_b + read_b;
						}
					}
				}
			}
			System.out.println("[iNFO:]ITEMS WITH NO ADDED STOCK:" + items_with_no_added_stock);

			databaseCrudClient.closeClient();
			ArrayList<String> dataHeader = new ArrayList<String>();
			dataHeader.add("item_index");
			dataHeader.add("used_stock");
			dataHeader.add("end_stock");
			dataHeader.add("bought");
			dataHeader.add("out_of_stock");

			int index = 0;

			//      System.out.println("[INFO: ] Final item info collection size: "+items_FinalInfo.keySet().size());

			for (String it : items_FinalInfo.keySet()) {

				int item_id = Integer.parseInt(it);


				int[] item_d = items_FinalInfo.get(it);

				ArrayList<Object> data = new ArrayList<Object>();


				int stock = item_d[0] - item_d[2]; // (initial + added) minus the stock still in the DB = used stock
				data.add(stock);	  //used stock
				data.add(item_d[2]); //end stock
				data.add(item_d[1]); //bought


				int out_of_stock;
				out_of_stock = -(stock - item_d[1]);
				if (out_of_stock < 0) {
					System.out.println("Negative stock failure --  b: " + item_d[1] + " || es:" + item_d[2]);
				}
				data.add(out_of_stock);
				total_out_stock += out_of_stock;

				globalResultHandler.recordData("BUYING RESULTS", item_id + "", data);

				index++;
			}

			globalResultHandler.setDataHeader("BUYING RESULTS", dataHeader);
			System.out.println("OUT OF STOCK: " + total_out_stock);
		} else {
			BenchmarkSlave.terminated = true;
		}


		GregorianCalendar date = new GregorianCalendar();
		String data_string = date.get(GregorianCalendar.YEAR) + "\\" + (date.get(GregorianCalendar.MONTH) + 1) + "\\" + date.get(GregorianCalendar.DAY_OF_MONTH) + " -- " + date.get(GregorianCalendar.HOUR_OF_DAY) + ":" + date.get(GregorianCalendar.MINUTE) + "";
		globalResultHandler.getBechmark_info().put("End", data_string);

		globalResultHandler.listDataToSOutput();

		globalResultHandler.listDatatoFiles(result_path, "", true);


	}

}
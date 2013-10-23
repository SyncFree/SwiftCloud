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

import org.uminho.gsd.benchmarks.benchmark.BenchmarkNodeID;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.interfaces.Entity;
import org.uminho.gsd.benchmarks.interfaces.Workload.Operation;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;

import java.util.List;
import java.util.Map;

/**
 * The class that implements this interface should run the operations given by
 * the {@linkplain WorkloadGeneratorInterface workload generation client} , and
 * store important results in the result handler
 */
public interface DatabaseExecutorInterface {

	/**
	 * Start the benchmark process within the client.<br>
	 * This operation should always be sequential, relying in the Executor class
	 * that handles the thread clients <br>
	 * 
	 * @param workload
	 *            the workload generator that supplies the methods to be
	 *            executed.
	 * @param nodeId
	 *            the benchmark node where the client is executing.
	 * @param operation_number
	 *            the number of operation to execute.
	 */
	public void start(WorkloadGeneratorInterface workload,
			BenchmarkNodeID nodeId, int operation_number, ResultHandler handler);

	/**
	 * Start the benchmark process within the client.<br>
	 * This operation should always be sequential, relying in the Executor class
	 * that handles the thread clients <br>
	 * 
	 * @param op
	 *            operation to be executed, and where the result is stored.
	 */
	public void execute(Operation op) throws Exception;

	public Object insert(String key, String path, Entity value)
			throws Exception;

	public void remove(String key, String path, String column) throws Exception;

	public void update(String key, String path, String column, Object value,
			String superfield) throws Exception;

	public Object read(String key, String path, String column, String superfield)
			throws Exception;

	public Map<String, Map<String, Object>> rangeQuery(String table,
			List<String> fields, int limit) throws Exception;

	public void truncate(String path) throws Exception;

	public void index(String key, String path, Object value) throws Exception;

	public void index(String key, String path, String indexed_key,
			Map<String, Object> value) throws Exception;

	public void closeClient();

	// info method
	public Map<String, String> getInfo();


}

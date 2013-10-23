/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package pt.citi.cs.crdt.benchmarks.tpcw.synchronization;

import java.util.HashMap;
import java.util.Map;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class TPCWRpc implements RpcMessage {

	private long startTime, receivedTime, opExecutionEndTime, totalTime, threadID;
	private String opName;
	private byte rawData[];
	private boolean finished;
	private static byte[] rawContent = new byte[64000];
	

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	TPCWRpc() {
	}

	public TPCWRpc(long startTime) {
		this.startTime = startTime;
	}

	public void setOperation(long opExecutionEndTime, String opName,
			long threadID) {
		MessageSize messageSize = MessageSize.valueOf(opName);

		this.opName = opName;
		this.threadID = threadID;
		this.opExecutionEndTime = opExecutionEndTime;
		this.rawData = new byte[messageSize.size()];
		
		int pivot = (int )Math.random()*rawData.length;
		
		for(int i=pivot; i< messageSize.size();i++)
			rawData[i-pivot] = rawContent[i%rawContent.length];
		
	}

	/*
	 * public void setReceivedTime(long receivedTime) { this.receivedTime =
	 * receivedTime; }
	 */

	public void setTotalTime(long totalTime) {
		this.totalTime = totalTime;
	}

	public String toString() {
		String s = String.format("%d\t%s\t%d\t%d\t%d\t%d", threadID, opName,
				startTime, receivedTime, opExecutionEndTime, totalTime);
		return s;
	}

	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		if (handle.expectingReply())
			((TPCWRpcHandler) handler).onReceive(handle, this);
		else
			((TPCWRpcHandler) handler).onReceive(this);
	}

	public void setReceivedTime(long currentTimeMillis) {
		this.receivedTime = currentTimeMillis;

	}

	public TPCWRpc(TPCWRpc otherMessage) {
		this.finished = otherMessage.finished;
		this.opExecutionEndTime = otherMessage.opExecutionEndTime;
		this.opName = otherMessage.opName;
		this.rawData = otherMessage.rawData;
		this.receivedTime = otherMessage.receivedTime;
		this.startTime = otherMessage.startTime;
		this.threadID = otherMessage.threadID;
		this.totalTime = otherMessage.totalTime;
	}

}
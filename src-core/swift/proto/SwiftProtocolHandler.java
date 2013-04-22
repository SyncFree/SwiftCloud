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
package swift.proto;

import java.util.logging.Logger;

import sys.dht.api.DHT;
import sys.dht.api.DHT.Handle;
import sys.dht.api.DHT.Reply;
import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcMessage;
import sys.pubsub.PubSubNotification;

/**
 * 
 * @author smduarte
 * 
 */
public class SwiftProtocolHandler extends AbstractRpcHandler implements DHT.ReplyHandler {
    private static Logger logger = Logger.getLogger(SwiftProtocolHandler.class.getName());

    protected void onReceive(RpcHandle conn, UnsubscribeUpdatesReply request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, UnsubscribeUpdatesRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, FetchObjectVersionRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, FetchObjectVersionReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, FetchObjectDeltaRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, LatestKnownClockRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, LatestKnownClockReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, CommitTSRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, CommitTSReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, BatchCommitUpdatesRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, BatchCommitUpdatesReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, CommitUpdatesRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, CommitUpdatesReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, GenerateDCTimestampRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, GenerateDCTimestampReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, final SeqCommitUpdatesRequest request) {
        Thread.dumpStack();
    }

    protected void onReceive(RpcHandle conn, final SeqCommitUpdatesReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(DHT.Handle conn, DHTExecCRDTReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(DHTExecCRDTReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(DHT.Handle conn, DHTGetCRDTReply reply) {
        Thread.dumpStack();
    }

    protected void onReceive(DHTGetCRDTReply reply) {
        Thread.dumpStack();
    }

    public void onReceive(RpcHandle conn, PubSubNotification notification) {
        Thread.dumpStack();
    }

    @Override
    public void onReceive(RpcMessage m) {
        System.err.println("-------------------->" + getClass());
        logger.warning("unhandled RPC message " + m);
    }

    @Override
    public void onReceive(RpcHandle handle, RpcMessage m) {
        logger.warning("unhandled RPC message " + m);
    }

    @Override
    public void onReceive(Reply m) {
        logger.warning("unhandled DHT message " + m);
    }

    @Override
    public void onReceive(Handle conn, Reply m) {
        logger.warning("unhandled DHT message " + m);
    }

    @Override
    public void onFailure() {
    }
}

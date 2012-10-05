package sys.net.impl.rpc;

import static sys.Sys.Sys;
import static sys.net.impl.NetworkingConstants.RPC_MAX_SERVICE_ID;
import static sys.net.impl.NetworkingConstants.RPC_MAX_TIMEOUT;

import java.util.logging.Logger;


import sys.net.api.Endpoint;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.utils.Threading;

final public class RpcPacket extends AbstractRpcPacket {

	private static Logger Log = Logger.getLogger( RpcPacket.class.getName() );


	RpcFactoryImpl fac;
	boolean isWaiting4Reply = false;
	boolean deferredRepliesEnabled = false;

	long rtt;
	
	RpcPacket() {
	}

	RpcPacket(RpcFactoryImpl fac, long service, RpcHandler handler) {
		this.fac = fac;
		this.timeout = 0;
		this.handler = handler;
		this.handlerId = service;
		this.replyHandlerId = service;
		fac.handlers0.put(handlerId, this);
	}

	RpcPacket(RpcFactoryImpl fac, Endpoint remote, RpcMessage payload, RpcPacket handle, RpcHandler replyhandler, int timeout) {
		this.fac = fac;
		this.remote = remote;
		this.timeout = timeout;
		this.payload = payload;
		this.handler = replyhandler;
		this.handlerId = handle.replyHandlerId;

		if (replyhandler != null) {
			synchronized (fac.handlers1) {
				this.replyHandlerId = ++g_handlers;
				fac.handlers1.put(this.replyHandlerId, this);
			}
			this.timestamp = Sys.timeMillis();
		} else
			this.replyHandlerId = 0L;
	}

	@Override
	public Endpoint localEndpoint() {
		return fac.facEndpoint;
	}

	@Override
	public RpcHandle send(Endpoint remote, RpcMessage msg, RpcHandler replyHandler, int timeout) {
		Log.finest("Sending: " + msg + " to " + remote );

		RpcPacket pkt = new RpcPacket(fac, remote, msg, this, replyHandler, timeout);
		if (timeout != 0)
			synchronized (pkt) {
				pkt.isWaiting4Reply = true;
				if (pkt.sendRpcSuccess(null, this))
					pkt.waitForReply();
			}
		else {
			pkt.remote = remote;
			pkt.sendRpcSuccess(null, this);
		}
		return pkt;
	}

	public RpcHandle reply(RpcMessage msg, RpcHandler replyHandler, int timeout) {
//		Log.finest("Replying: " + msg + " to " + remote );
		RpcPacket pkt = new RpcPacket(fac, remote(), msg, this, replyHandler, timeout);

		if (timeout != 0)
			synchronized (pkt) {
				// System.out.println("sync for:" + pkt.hashCode() );
				pkt.isWaiting4Reply = true;
				if (pkt.sendRpcSuccess(conn, this))
					pkt.waitForReply();
			}
		else
			pkt.sendRpcSuccess(conn, this);
		return pkt;
	}

	final void deliver(AbstractRpcPacket pkt) {
		this.rtt = Sys.timeMillis() - timestamp;
		
		if (isWaiting4Reply) {
			synchronized (this) {
				reply = pkt;
				Threading.notifyAllOn(this);
			}
		} else {
			if( this.handler != null )
				pkt.payload.deliverTo(pkt, this.handler);
			else
				Log.warning(String.format("Cannot handle RpcPacket: %s from %s, reason handler is null", pkt.getClass(), pkt.remote() ) );
		}
	}

	final private void waitForReply() {
		while (reply == null && !timedOut());

		isWaiting4Reply = false;
		if (reply != null)
			reply.payload.deliverTo(reply, this.handler);
		
	}

	final private boolean timedOut() {
		int ms = (int) ((timeout < 0 ? RPC_MAX_TIMEOUT : timeout) - (Sys.timeMillis() - timestamp));
		if (ms > 0)
			Threading.waitOn(this, ms > 100 ? 100 : ms);
		return ms <= 0;
	}

	private boolean sendRpcSuccess(TransportConnection conn, AbstractRpcPacket handle) {
		try {
			if (conn != null && conn.send(this) || fac.conMgr.send(remote(), this)) {
				payload = null;
				return true;
			} else {
				if (handler != null)
					handler.onFailure(this);
				else if (handle.handler != null)
					handle.handler.onFailure(this);

				return false;
			}
		} catch (Throwable t) {
			failed = true;
			failureCause = t;

			if (handler != null)
				handler.onFailure(this);
			else
				handle.handler.onFailure(this);

			return false;
		}
	}

	@Override
	public RpcHandle enableDeferredReplies(int timeout) {
		deferredRepliesEnabled = timeout > 0;
		synchronized (fac.handlers1) {
			fac.handlers1.remove(handlerId);
			fac.handlers0.put(handlerId, this);
		}
		return this;
	}

	public String toString() {
		return payload != null ? payload.getClass().toString() : String.format("RPC(%s,%s,%s)", handlerId, replyHandlerId, this.handler);
	}

	@Override
	public void deliverTo(TransportConnection conn, MessageHandler handler) {
		((RpcFactoryImpl) handler).onReceive(conn, this);
	}
	
	// [0-MAX_SERVICE_ID[ are reserved for static service handlers.
	static long g_handlers = RPC_MAX_SERVICE_ID;

	@Override
	public RpcFactory getFactory() {
		return fac;
	}

}
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
package sys.dht.catadupa;

import static sys.Sys.Sys;
import static sys.dht.catadupa.Config.Config;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import sys.dht.catadupa.crdts.time.Timestamp;
import sys.dht.catadupa.msgs.CatadupaCast;
import sys.dht.catadupa.msgs.CatadupaCastPayload;
import sys.dht.catadupa.msgs.CatadupaHandler;
import sys.dht.catadupa.msgs.DbMergeReply;
import sys.dht.catadupa.msgs.DbMergeRequest;
import sys.dht.catadupa.msgs.JoinRequest;
import sys.dht.catadupa.msgs.JoinRequestAccept;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcHandle;
import sys.scheduler.PeriodicTask;
import sys.scheduler.Task;

/**
 * 
 * @author smd
 * 
 */
public class CatadupaNode extends LocalNode implements MembershipListener {

	private static Logger Log = Logger.getLogger( CatadupaNode.class.getName() );

	protected DB db;
	Task sequencerTask, repairTask;

	public double lastSequencerRun = Double.NEGATIVE_INFINITY;
	public double lastDepartureSend = Double.NEGATIVE_INFINITY;

	double targetFanout = Config.BROADCAST_MAX_FANOUT;

	Set<Node> joins = Collections.synchronizedSet(new HashSet<Node>());
	Set<Node> rejoins = Collections.synchronizedSet(new HashSet<Node>());
	Set<Node> departures = Collections.synchronizedSet(new HashSet<Node>());

	public CatadupaNode() {
	}

	public void init() {
		db = new DB(this);

		Log.finer("I am " + db.self);

		initSequencerTask();
		repairCatadupa();
		joinCatadupa();

		new PeriodicTask(0, 5) {
			@Override
			public void run() {
				Log.finer(self.key + ": " + db.k2n.values());
			}
		};
	}

	public boolean isReady() {
		return db.joined;
	}

	// --------------------------------------------------------------------------------------------------------------------------------
	void joinCatadupa() {

		new PeriodicTask(0, 5 + 0 * Config.JOIN_ATTEMPT_PERIOD) {
			double backoff = 0.1;

			@Override
			public void run() {
				if (!db.joined) {
					JoinRequest jr = new JoinRequest(db.self);
					Node aggregator = db.aggregatorFor(0, self.key);
					rpc.send(aggregator.endpoint, jr, new CatadupaHandler() {

						@Override
						public void onReceive(JoinRequestAccept m) {
							reSchedule(Config.JOIN_ATTEMPT_PERIOD);
						}

						@Override
						public void onFailure(RpcHandle handle) {
							backoff = Math.min(5, backoff * 1.5);
							reSchedule(backoff);
						}
					});
				} else
					cancel();
			}
		};
	}

	@Override
	public void onReceive(RpcHandle handle, JoinRequest m) {
		joins.add(m.node);
		handle.reply(new JoinRequestAccept());
		if (!sequencerTask.isScheduled())
			sequencerTask.reSchedule(Config.SEQUENCER_BROADCAST_PERIOD + Sys.rg.nextDouble());
	}

	// --------------------------------------------------------------------------------------------------------------------------------
	void initSequencerTask() {
		sequencerTask = new Task(Config.SEQUENCER_BROADCAST_PERIOD) {
			@Override
			public void run() {
				if (joins.size() > 0 || rejoins.size() > 0 || departures.size() > 0) {
					lastSequencerRun = Sys.currentTime();

					broadcastCatadupaUpdates();

					joins.clear();
					rejoins.clear();
					departures.clear();
				}
			}
		};
	}

	private void broadcastCatadupaUpdates() {
		MembershipUpdate m = new MembershipUpdate(joins, departures, rejoins);
		Timestamp ts = db.merge(m);

		Log.finest("Broacasting:" + m + "-->" + ts);

		CatadupaCastPayload ccp = new CatadupaCastPayload(m, ts);
		onReceive((RpcHandle) null, new CatadupaCast(0, self.key, new Range(), ccp));
	}

	// --------------------------------------------------------------------------------------------------------------------------------
	// Broadcast a membership aggregate event.
	@Override
	public void onReceive(RpcHandle sock, final CatadupaCast m) {

		final int BroadcastFanout = broadcastFanout(m.level);

		this.onReceive(sock, m.payload);

		Range r0 = m.range;

		if (m.level > 0)
			r0 = r0.advancePast(self.key);

		if (r0.sizeGreaterThan(BroadcastFanout, db)) {
			for (Range j : r0.slice(m.level, BroadcastFanout, db)) {
				for (Node i : j.nodes(db)) {
					if (i.key == m.rootKey)
						continue;

					if (rpc.send(i.endpoint, new CatadupaCast(m.level + 1, m.rootKey, j, m.payload)).succeeded())
						break;
				}
			}
		} else {
			for (Node i : r0.nodes(db))
				if (i.key != self.key && i.key != m.rootKey)
					rpc.send(i.endpoint, m.payload);
		}
	}

	final int broadcastFanout(int level) {
		return level == 0 ? 1 : Config.CATADUPA_DYNAMIC_FANOUT ? dynamicBroadcastFanout() : staticBroadcastFanout();
	}

	public int staticBroadcastFanout() {
		return Config.BROADCAST_MAX_FANOUT;
	}

	public int dynamicBroadcastFanout() {
		final double TARGET_BROADCAST_ADJUST = 0.05;

		targetFanout *= (1 + TARGET_BROADCAST_ADJUST * (Sys.uploadedBytes.get() > Sys.downloadedBytes.get() ? -1 : 1));
		targetFanout = Math.max(2, Math.min(targetFanout, Config.BROADCAST_MAX_FANOUT));

		int floor = (int) targetFanout;
		return floor + (Sys.rg.nextDouble() < (targetFanout - floor) ? 1 : 0);
	}

	/*
	 * 
	 * Process a new membership aggregate event. The node has joined when it is
	 * being announced in the payload. Updates the node membership database.
	 */
	@Override
	public void onReceive(RpcHandle call, CatadupaCastPayload m) {
		Log.finest(self.key + "  CatadupaCastPayload: " + m.data);
		db.merge(m);
	}

	// ------------------------------------------------------------------------------------------------
	// EPIDEMIC MEMBERSHIP REPAIR
	/*
	 * Do periodic pair-wise membership (incremental) merge operations.
	 */
	double mergePeriod = Config.MEMBERSHIP_MERGE_PERIOD;
	void repairCatadupa() {
		repairTask = new Task(mergePeriod + Sys.rg.nextDouble()) {
			double backoff = 0.1;

			@Override
			public void run() {
				Node other = db.randomNode();
				Log.finest(self.key + "-->Merging with:" + other);
				if (other != null) {
					rpc.send(other.endpoint, new DbMergeRequest(db.clock()), new CatadupaHandler() {

						@Override
						public void onFailure(RpcHandle handle) {
							backoff = Math.min(5, backoff * 1.5);
							reSchedule(backoff);
						}

						@Override
						public void onReceive(final RpcHandle handle, final DbMergeReply r) {
							backoff = 0.1;

							Log.finest(self.key + " MyClock:" + db.clock() + " OtherClock:" + r.clock);
							Log.finest("--DB---->" + db.membership );

							db.merge(r);
							Map<MembershipUpdate, Timestamp> delta = db.delta( r.clock) ;
							if( delta.size() > 0 )
								handle.reply(new DbMergeReply(db.clock(), delta));

							mergePeriod = Math.min(45, Math.max(10, mergePeriod));
							reSchedule(mergePeriod + Sys.rg.nextDouble());
						}
					}, 250);
				}
				reSchedule(mergePeriod + Sys.rg.nextDouble());
			}
		};
	}

	@Override
	public void onReceive(RpcHandle handle, DbMergeRequest other) {

		Log.finest(self.key + " MyClock:" + db.clock() + " OtherClock:" + other.clock);
		Log.finest("---DB--->" + db.membership);

		Map<MembershipUpdate, Timestamp> delta = db.delta( other.clock ) ;
		Log.finest("Delta:" + delta);

		handle.reply(new DbMergeReply(db.clock(), delta), new CatadupaHandler() {

			@Override
			public void onReceive(DbMergeReply r) {
				db.merge(r);
				repairTask.reSchedule(mergePeriod + Sys.rg.nextDouble());
			}
		}, 250);
		repairTask.reSchedule(mergePeriod + Sys.rg.nextDouble());
	}

	// --------------------------------------------------------------------------------------------------------------------------------

	// Failed Nodes...

	/*
	 * Handle node failure events...
	 */
	@Override
	public void onFailure(RpcHandle handle) {
		Endpoint remote = handle.remoteEndpoint();
		if (remote != endpoint) {

			Node failedNode = new Node(remote);

			Log.finest("Failed Node:" + failedNode);
			// if (state.db.loadedEndpoints) {
			// state.db.deadNodes.set(failedNode.offline_index);
			// state.departures.add(failedNode.offline_index);
			//
			// if (!isSequencer() && state.departures.size() > 0 &&
			// (currentTime() - state.lastDepartureSend) > 10) {
			// FailureNotice r = new FailureNotice(state.departures);
			// CatadupaNode sequencer = state.db.sequencerFor(r.level,
			// this.key);
			// lPriority.send(sequencer.endpoint, r, 0, new
			// CatadupaReplyHandler());
			// state.lastDepartureSend = currentTime();
			// state.departures.clear();
			// }
			// }
		}
	}

	@Override
	public void onNodeAdded(Node n) {
		Thread.dumpStack();
	}

	@Override
	public void onNodeRemoved(Node n) {
		Thread.dumpStack();
	}
}

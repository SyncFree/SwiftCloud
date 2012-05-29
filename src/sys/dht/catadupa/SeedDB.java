package sys.dht.catadupa;

import static sys.utils.Log.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sys.dht.catadupa.riak.Riak;
import sys.dht.discovery.Discovery;
import sys.net.api.Endpoint;

/**
 * 
 * @author smd
 * 
 */
public class SeedDB {

	static String RIAK_KEY = "seedDB";
	static String RIAK_BUCKET = "swift.catadupa";

	static List<Node> seeds = new ArrayList<Node>();

	public static void addSeedNode(Endpoint endpoint) {
		seeds.add(new Node(endpoint));
	}

	public static RandomList<Node> nodes() {
		return new RandomList<Node>(seeds);
	}

	public static Node randomSeedNode() {
		return new RandomList<Node>(seeds).randomElement();
	}

	static void initWithRiak(Node self) {
		Collection<Node> riakSeeds = Riak.load(RIAK_BUCKET, RIAK_KEY);
		if (riakSeeds != null) {
			seeds.addAll(riakSeeds);
		} else {
			seeds.add(self);
			Riak.store(RIAK_BUCKET, RIAK_KEY, seeds);
		}

		// Riak.delete(SeedDB.RIAK_BUCKET, SeedDB.RIAK_KEY);
	}

	static void initWithMulticast(Node self) {
		Endpoint seed = Discovery.lookup(Catadupa.discoveryName(), 1000);
		if (seed != null) {
			Log.finer(String.format("Seed node found in local machine/network: <%s>", seed));
			seeds.add(new Node(seed));
		} else {
			if (seeds.isEmpty()) {
				Log.finer("No seed node found in local machine/network");
				seeds.add(self);
				Discovery.register(Catadupa.discoveryName(), self.endpoint);
			} else {
				seed = seeds.get(0).endpoint;
				Log.finer(String.format("Using default node: <%s>", seed));
			}
		}
	}

	static void init(final Node self) {
		initWithMulticast(self);
	}
}

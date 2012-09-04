package sys.dht.catadupa;

/**
 * 
 * @author smd
 * 
 */
abstract public class Config {
	// Default config values...
	// Do not alter them directly, use a subclass...

	public int NODE_KEY_LENGTH = 10;

	public boolean CATADUPA_DYNAMIC_FANOUT = true;
	public double CATADUPA_LOAD_BALANCE_FACTOR = 1e10;

	public double JOIN_ATTEMPT_PERIOD = 10.0;
	public double MEMBERSHIP_MERGE_PERIOD = 10.0;
	public double SEQUENCER_BROADCAST_PERIOD = 5.0;

	public int PUBSUB_MAX_FANOUT = 3;
	public int BROADCAST_MAX_FANOUT = 4;

	public int JOINS_AGGREGATION_DEPTH = 3;
	public int EXITS_AGGREGATION_DEPTH = 6;

	public double JOINS_TARGET_AGGREGATION_RATE = 0.5;
	public double EXITS_TARGET_AGGREGATION_RATE = 1;

	public int VIEW_CUTOFF_WINDOW = 60 * 60;
	public int VIEW_CUTOFF = (int) (VIEW_CUTOFF_WINDOW / SEQUENCER_BROADCAST_PERIOD);

	public double CAT_H_CAPACITY = 24 * 1024;
	public double CAT_L_CAPACITY = 24 * 1024;

	public double CAT_H_PEAK_CAPACITY = 50 * 1024;
	public double CAT_L_PEAK_CAPACITY = 50 * 1024;

	public String Home = System.getProperty("user.home");
	public String StatsOutputFile = Home + "/runs/" + getClass().getName() + "-stats.xml";
	public String ConfigOutputFile = Home + "/runs/" + getClass().getName() + "-config.xml";

	public static Config Config = null;

	protected void init() {
	}

	static {
		Config = new Config() {
		};
	}
}
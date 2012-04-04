package sys.dht.catadupa;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * 
 * @author smd
 * 
 */
public class MembershipUpdate {

	public Node[] arrivals, departures, rejoins;

	public MembershipUpdate() {
	}

	public MembershipUpdate(Node n) {
		arrivals = new Node[] { n };
		departures = rejoins = new Node[] {};
	}

	public MembershipUpdate(Set<Node> arrivals, Set<Node> departures, Set<Node> rejoins) {
		this.rejoins = rejoins.toArray(new Node[rejoins.size()]);
		this.arrivals = arrivals.toArray(new Node[arrivals.size()]);
		this.departures = departures.toArray(new Node[departures.size()]);
	}

	public List<Node> arrivals() {
		return Arrays.asList(arrivals);
	}

	public List<Node> rejoins() {
		return Arrays.asList(rejoins);
	}

	public List<Node> departures() {
		return Arrays.asList(departures);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		// sb.append( timeStamp ) ;
		sb.append(Arrays.asList(arrivals));
		return sb.toString();
	}
}

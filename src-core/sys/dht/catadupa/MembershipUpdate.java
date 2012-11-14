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

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
package sys.dht.catadupa.crdts.time;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class LVV implements CausalityClock<LVV, LVV.TS> {

	int maxCounter = -1;
	Map<String, TsSet> data;

	public LVV() {
		data = new LinkedHashMap<String, TsSet>();
	}

	private LVV(LVV other) {
		data = new LinkedHashMap<String, TsSet>();
		for (Map.Entry<String, TsSet> i : other.data.entrySet()) {
			data.put(i.getKey(), i.getValue().clone());
		}
	}

	@Override
	public TS recordNext(String siteId) {
		TsSet s = getSet(siteId);
		TS t = new TS(siteId, ++maxCounter, s.maxCounter());
		s.add(t);
		return t;
	}

	@Override
	public void record(TS t) {
		if (t.c_value > maxCounter)
			maxCounter = t.c_value;

		getSet(t.siteId).add(t);
	}

	@Override
	public boolean includes(TS t) {
		TsSet s = data.get(t.siteId);
		return s != null && s.contains(t);
	}

	@Override
	public CMP_CLOCK compareTo(LVV other) {
		boolean lessThan = false; // this less than c
		boolean greaterThan = false;

		Iterator<Map.Entry<String, TsSet>> it;

		for (it = other.data.entrySet().iterator(); it.hasNext() && !lessThan;) {
			Map.Entry<String, TsSet> e = it.next();
			TsSet s = data.get(e.getKey());
			lessThan |= s == null || !s.containsAll(e.getValue());
		}

		for (it = data.entrySet().iterator(); it.hasNext() && !greaterThan;) {
			Map.Entry<String, TsSet> e = it.next();
			TsSet s = other.data.get(e.getKey());
			greaterThan |= s == null || (!s.containsAll(e.getValue()));
		}

		if (greaterThan && lessThan) {
			return CMP_CLOCK.CMP_CONCURRENT;
		}
		if (greaterThan) {
			return CMP_CLOCK.CMP_DOMINATES;
		}
		if (lessThan) {
			return CMP_CLOCK.CMP_ISDOMINATED;
		}
		return CMP_CLOCK.CMP_EQUALS;
	}

	@Override
	public CMP_CLOCK merge(LVV other) {
		boolean lessThan = false; // this less than c
		boolean greaterThan = false;

		for (Map.Entry<String, TsSet> i : other.data.entrySet()) {
			lessThan |= getSet(i.getKey()).addAll(i.getValue());
		}

		for (Map.Entry<String, TsSet> i : data.entrySet()) {
			TsSet s = other.data.get(i.getKey());
			greaterThan |= s == null || (!s.containsAll(i.getValue()));
		}

		if (greaterThan && lessThan) {
			return CMP_CLOCK.CMP_CONCURRENT;
		}
		if (greaterThan) {
			return CMP_CLOCK.CMP_DOMINATES;
		}
		if (lessThan) {
			return CMP_CLOCK.CMP_ISDOMINATED;
		}
		return CMP_CLOCK.CMP_EQUALS;
	}

	@Override
	public LVV clone() {
		return new LVV(this);
	}

	private TsSet getSet(String site) {
		TsSet res = data.get(site);
		if (res == null)
			data.put(site, res = new TsSet());
		return res;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (Map.Entry<String, TsSet> i : data.entrySet()) {
			sb.append(String.format("<%s : %s>", i.getKey(), i.getValue())).append(", ");
		}
		if (data.size() > 0)
			sb.delete(sb.length() - 2, sb.length());
		sb.append('}');
		return sb.toString();
	}

	public static class TS implements Timestamp {

		String siteId;
		public int c_value;
		public int p_value;

		public TS() {
		}

		TS(TS other) {
			siteId = other.siteId;
			c_value = other.c_value;
			p_value = other.p_value;
		}

		TS(String siteId, int c_value, int p_value) {
			this.siteId = siteId;
			this.c_value = c_value;
			this.p_value = p_value;
		}

		@Override
		public String siteId() {
			return siteId;
		}

		@Override
		public TS clone() {
			return new TS(this);
		}

		@Override
		public int size() {
			return 2 * (Integer.SIZE / Byte.SIZE) + siteId.length();
		}

		public int compareTo(TS other) {
			return (c_value != other.c_value) ? Integer.signum(c_value - other.c_value) : siteId.compareTo(other.siteId);
		}

		@Override
		public String toString() {
			return String.format("%d (%d)", c_value, p_value);
		}

		@Override
		public int compareTo(Timestamp other) {
			return compareTo((TS) other);
		}

		@Override
		public int hashCode() {
			return c_value ^ p_value;
		}

		public boolean equals(TS other) {
			return c_value == other.c_value && siteId.equals(other.siteId);
		}

		@Override
		public boolean equals(Object other) {
			return equals((TS) other);
		}

		@Override
		public void recordIn(CausalityClock<?, ?> cc) {
			((LVV) cc).record(this);
		}

		@Override
		public boolean includedIn(CausalityClock<?, ?> cc) {
			return ((LVV) cc).includes(this);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
	}

	public static class TsSet extends TreeSet<LVV.TS> {

		public TsSet() {
		}

		TsSet(TsSet other) {
			super(other);
		}

		int maxCounter() {
			return isEmpty() ? -1 : last().c_value;
		}

		@Override
		public TsSet clone() {
			return new TsSet(this);
		}

		TsSet trim(int cutoff) {
			for (Iterator<LVV.TS> i = iterator(); i.hasNext();)
				if (i.next().c_value <= cutoff)
					i.remove();
			return this;
		}

		List<LVV.TS> trim() {
			List<LVV.TS> res = new ArrayList<LVV.TS>();

			LVV.TS[] sa = super.toArray(new LVV.TS[size()]);

			int i = 0;
			for (; i < sa.length - 1 && sa[i + 1].p_value == sa[i].c_value; i++)
				;

			for (; i < sa.length; i++)
				res.add(sa[i]);

			return res;
		}

		public boolean contains(LVV.TS s, int cutoff) {
			return s.c_value <= cutoff || super.contains(s);
		}

		// public boolean contains(Object other) {
		// Thread.dumpStack(); // vai ser preciso por causa do cutoff
		// return false;
		// }

		public boolean containsAll(Collection<?> c, int cutoff) {
			for (Object i : c)
				if (((LVV.TS) i).c_value > cutoff && !super.contains(i))
					return false;

			return true;
		}

		@Override
		public String toString() {
			return super.toString();
		}

		private static final long serialVersionUID = 1L;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Collection<TS> delta(LVV other) {
		Collection<TS> res = new ArrayList<TS>();
		for (Iterator<Map.Entry<String, TsSet>> it = data.entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, TsSet> e = it.next();
			TsSet otherSet = other.data.get(e.getKey());
			for (TS t : e.getValue())
				if (otherSet == null || !otherSet.contains(t))
					res.add(t);
		}
		return res;
	}
}

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
package swift.clocks;

/**
 * Timestamp generator for a given site. Always generates the max from the
 * consecutive counter for the given site and a given causality clock.
 * 
 * NOTE: if the given clock is updated outside of this class, the next clock
 * will take this into consideration.
 * 
 * @author nmp
 * 
 */
public class CCIncrementalTimestampGenerator implements TimestampSource<Timestamp> {

    private String siteid;
    private CausalityClock clock;
    private long last;

    public CCIncrementalTimestampGenerator(String siteid, CausalityClock clock) {
        this(siteid, clock, Timestamp.MIN_VALUE);
    }

    public CCIncrementalTimestampGenerator(String siteid, CausalityClock clock, long last) {
        if (siteid == null) {
            throw new NullPointerException();
        }
        this.siteid = siteid;
        this.clock = clock;
        this.last = last;
    }

    @Override
    public synchronized Timestamp generateNew() {
        last = Math.max(last, clock.getLatestCounter(siteid));
        return new Timestamp(siteid, ++last);
    }

}

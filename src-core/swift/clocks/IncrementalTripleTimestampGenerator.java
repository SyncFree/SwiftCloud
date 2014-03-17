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
 * IncrementalTripleTimestampGenerator generator based on a timestamp.
 * 
 * @author nmp, smduarte, mzawirski
 * 
 */
public class IncrementalTripleTimestampGenerator implements TimestampSource<TripleTimestamp> {

    protected Timestamp clientTimestamp;
    protected long last;

    // for Kryo
    IncrementalTripleTimestampGenerator() {
    }

    public IncrementalTripleTimestampGenerator(Timestamp clientTimestamp) {
        this.clientTimestamp = clientTimestamp;
        this.last = Timestamp.MIN_VALUE;
    }

    @Override
    public synchronized TripleTimestamp generateNew() {
        return new TripleTimestamp(clientTimestamp, ++last);
    }

}

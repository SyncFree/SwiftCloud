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
 * TimestampSource decorator that allows to explicitly return and later reuse
 * last generated timestamp. Useful for generating continuous sequences for
 * CausalityClock when some timestamps are not included in the CausalityClock -
 * they can be returned to the generator instead.
 * 
 * @author mzawirski
 */
public class ReturnableTimestampSourceDecorator<T extends Timestamp> implements TimestampSource<T> {
    private final TimestampSource<T> origSource;
    private boolean lastTimestampReturned;
    private T lastTimestamp;

    public ReturnableTimestampSourceDecorator(TimestampSource<T> origSource) {
        this.origSource = origSource;
    }

    @Override
    public T generateNew() {
        if (lastTimestampReturned) {
            lastTimestampReturned = false;
            return lastTimestamp;
        }
        lastTimestamp = origSource.generateNew();
        return lastTimestamp;
    }

    public void returnLastTimestamp() {
        if (lastTimestamp != null) {
            lastTimestampReturned = true;
        }
    }
}

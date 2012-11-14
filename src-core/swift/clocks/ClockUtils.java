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

import swift.clocks.CausalityClock.CMP_CLOCK;

public class ClockUtils {

    public static CMP_CLOCK combineCmpClock(CMP_CLOCK c1, CMP_CLOCK c2) {
        if (c1 == CMP_CLOCK.CMP_EQUALS) {
            return c2;
        } else if (c2 == CMP_CLOCK.CMP_EQUALS) {
            return c1;
        } else if (c1 == CMP_CLOCK.CMP_CONCURRENT) {
            return c1;
        } else if (c2 == CMP_CLOCK.CMP_CONCURRENT) {
            return c2;
        } else if (c1 == CMP_CLOCK.CMP_DOMINATES) {
            if (c2 == CMP_CLOCK.CMP_DOMINATES) {
                return CMP_CLOCK.CMP_DOMINATES;
            } else {
                return CMP_CLOCK.CMP_CONCURRENT;
            }
        } else { // c1 == CMP_CLOCK.CMP_ISDOMINATED
            if (c2 == CMP_CLOCK.CMP_ISDOMINATED) {
                return CMP_CLOCK.CMP_ISDOMINATED;
            } else {
                return CMP_CLOCK.CMP_CONCURRENT;
            }
        }

    }
}

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
 * Using the ClockFactory and hiding all the actual constructors of the
 * different causality clocks, we enforce that there is only one kind of version
 * vector used.
 * 
 * For now, we only use VersionVectorWithExceptions!
 * 
 * TODO Make the choice for the kind of version vector part of the configuration
 * file.
 * 
 * @author annettebieniusa
 * 
 */
public class ClockFactory {
    public static CausalityClock newClock() {
        return new VersionVectorWithExceptions();
    }

    public static CausalityClock newClock(CausalityClock c) {
        if (c instanceof VersionVectorWithExceptions) {
            return new VersionVectorWithExceptions((VersionVectorWithExceptions) c);
        } else {
            throw new RuntimeException(
                    "This should not have happened! The system only uses VersionVectorWithExceptions!");
        }

    }

}
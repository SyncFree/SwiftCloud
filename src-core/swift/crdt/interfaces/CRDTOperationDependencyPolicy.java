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
package swift.crdt.interfaces;

/**
 * Policy defining how to treat operation group dependencies when attempting to
 * execute operation group on a CRDT object.
 * 
 * @author mzawirski
 */
public enum CRDTOperationDependencyPolicy {
    /**
     * Check operation group dependencies when trying to execute it, fail if
     * they are not me.
     */
    CHECK,
    /**
     * Do not check operation group dependencies when trying to execute it,
     * assume they are met and record dependencies clock as if they were already
     * included in the state.
     */
    RECORD_BLINDLY,
    /**
     * Do not check operation group dependencies when trying to execute it.
     */
    IGNORE
}
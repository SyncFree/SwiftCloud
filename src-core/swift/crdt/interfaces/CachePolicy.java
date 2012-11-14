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
 * Cache policies used by client for transactional reads.
 * 
 * @author annettebieniusa
 * 
 */
public enum CachePolicy {
    /**
     * Always get the most recent version allowed by {@link IsolationLevel},
     * fail if communication is impossible.
     */
    STRICTLY_MOST_RECENT,
    /**
     * Try to get the most recent version allowed by {@link IsolationLevel}, if
     * impossible fall back to reuse object from the local cache. Fail if cache
     * does not contain compatible version.
     */
    MOST_RECENT,
    /**
     * Reuse object from the local cache whenever possible, yielding possibly
     * stale versions. Minimises connections with server. Fail if cache does not
     * contain compatible version.
     */
    CACHED;
    /*
     * TODO(mzawirski): it seems a common requirement is to have something like
     * CACHED+trigger temporary updates subscription; CACHED mode without
     * updates subscription is a pretty rare case.
     */
}

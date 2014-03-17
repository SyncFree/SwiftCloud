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
package swift.application.social;

import swift.crdt.core.CRDTIdentifier;

/**
 * Provides methods for generating CRDT Identifiers based on the class and type
 * of object.
 * 
 * See wsocial_shared.cc for scheme used in walter, based on MD5 hashing
 * 
 * TODO Implement the same scheme here?
 * 
 * @author annettebieniusa
 * 
 */

public class NamingScheme {

    /**
     * Generates a CRDT identifier for the user from login name.
     * 
     * @param loginName
     * @return CRDT identifier for user
     */
    public static CRDTIdentifier forUser(String loginName) {
        return new CRDTIdentifier("users", loginName);
    }

    /**
     * Generates a CRDT identifier for the messages from login name.
     * 
     * @param loginName
     * @return CRDT identifier for collection of messages
     */
    public static CRDTIdentifier forMessages(final String loginName) {
        return new CRDTIdentifier("messages", loginName);
    }

    /**
     * Generates a CRDT identifier for the friends from login name.
     * 
     * @param loginName
     * @return CRDT identifier for collection of friends
     */
    public static CRDTIdentifier forFriends(final String loginName) {
        return new CRDTIdentifier("friends", loginName);
    }

    /**
     * Generates a CRDT identifier for incoming friend requests from login name.
     * 
     * @param loginName
     * @return CRDT identifier for collection of incoming friend requests
     */
    public static CRDTIdentifier forInFriendReq(final String loginName) {
        return new CRDTIdentifier("inFriendReq", loginName);
    }

    /**
     * Generates a CRDT identifier for send friend requests from login name.
     * 
     * @param loginName
     * @return CRDT identifier for collection of outgoing friend requests
     */
    public static CRDTIdentifier forOutFriendReq(final String loginName) {
        return new CRDTIdentifier("outFriendReq", loginName);
    }

    /**
     * Generates a CRDT identifier for events from login name.
     * 
     * @param loginName
     * @return CRDT identifier for collection of events
     */

    public static CRDTIdentifier forEvents(String loginName) {
        return new CRDTIdentifier("events", loginName);
    }
}

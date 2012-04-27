package swift.application.social;

import swift.crdt.CRDTIdentifier;

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
    public static CRDTIdentifier forUser(final String loginName) {
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

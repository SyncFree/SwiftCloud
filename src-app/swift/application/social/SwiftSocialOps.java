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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import swift.crdt.AddWinsSetCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.stats.Tally;

// implements the social network functionality
// see wsocial_srv.h

public class SwiftSocialOps {

    private static Logger logger = Logger.getLogger("swift.social");

    // FIXME Add sessions? Local login possible? Cookies?
    private User currentUser;
    private SwiftSession server;
    private final IsolationLevel isolationLevel;
    private final CachePolicy cachePolicy;
    private final ObjectUpdatesListener updatesSubscriber;
    private final boolean asyncCommit;

    public SwiftSocialOps(SwiftSession clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean subscribeUpdates, boolean asyncCommit) {
        server = clientServer;
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.updatesSubscriber = subscribeUpdates ? TxnHandle.UPDATES_SUBSCRIBER : null;
        this.asyncCommit = asyncCommit;
    }

    public SwiftSession getSwift() {
        return server;
    }

    // FIXME Return type integer encoding error msg?
    public boolean login(String loginName, String passwd) {
        logger.info("Got login request from user " + loginName);

        // Check if user is already logged in
        if (currentUser != null) {
            if (loginName.equals(currentUser)) {
                logger.info(loginName + " is already logged in");
                return true;
            } else {
                logger.info("Need to log out user " + currentUser.loginName + " first!");
                return false;
            }
        }

        TxnHandle txn = null;
        try {
            // Check if user is known at all
            // FIXME Is login possible in offline mode?

            // ATTENZIONE ATTENZIONE, HACK!! Shall we perhaps do it at client
            // startup? I am not changing it now to make experiments comparable.
            final CachePolicy loginCachePolicy;
            if (isolationLevel == IsolationLevel.SNAPSHOT_ISOLATION && cachePolicy == CachePolicy.CACHED) {
                loginCachePolicy = CachePolicy.MOST_RECENT;
            } else {
                loginCachePolicy = cachePolicy;
            }
            txn = server.beginTxn(isolationLevel, loginCachePolicy, true);
            @SuppressWarnings("unchecked")
            User user = (User) (txn.get(NamingScheme.forUser(loginName), false, LWWRegisterCRDT.class,
                    updatesSubscriber)).getValue();

            // Check password
            // FIXME We actually need an external authentification mechanism, as
            // clients cannot be trusted.
            // In Walter, authentification is done on server side, within the
            // data center. Moving password (even if hashed) to the client is a
            // security breach.
            if (user != null) {
                if (user.password.equals(passwd)) {
                    currentUser = user;
                    logger.info(loginName + " successfully logged in");
                    commitTxn(txn);
                    return true;
                } else {
                    logger.info("Wrong password for " + loginName);
                }
            } else {
                logger.severe("User has not been registered " + loginName);
            }
        } catch (NoSuchObjectException e) {
            logger.severe("User " + loginName + " is not known");
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }

        return false;
    }

    public void logout(String loginName) {
        currentUser = null;
        // FIXME End session? handle cookies?
        logger.info(loginName + " successfully logged out");
    }

    // FIXME Return error code?
    void registerUser(final String loginName, final String passwd, final String fullName, final long birthday,
            final long date) {
        logger.info("Got registration request for " + loginName);
        // FIXME How do we guarantee unique login names?
        // WalterSocial suggests using dedicated (non-replicated) login server.

        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, CachePolicy.STRICTLY_MOST_RECENT, false);
            User newUser = registerUser(txn, loginName, passwd, fullName, birthday, date);
            logger.info("Registered user: " + newUser);
            // Here only synchronous commit, as otherwise the following tests
            // might fail.
            txn.commit();
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    public User registerUser(final TxnHandle txn, final String loginName, final String passwd, final String fullName,
            final long birthday, final long date) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        // FIXME How do we guarantee unique login names?
        // WalterSocial suggests using dedicated (non-replicated) login server.

        LWWRegisterCRDT<User> reg = txn.get(NamingScheme.forUser(loginName), true, LWWRegisterCRDT.class, null);

        User newUser = new User(loginName, passwd, fullName, birthday, true);
        reg.set((User) newUser.copy());

        // Construct the associated sets with messages, friends etc.
        txn.get(newUser.msgList, true, AddWinsSetCRDT.class, null);
        txn.get(newUser.eventList, true, AddWinsSetCRDT.class, null);
        txn.get(newUser.friendList, true, AddWinsSetCRDT.class, null);
        txn.get(newUser.inFriendReq, true, AddWinsSetCRDT.class, null);
        txn.get(newUser.outFriendReq, true, AddWinsSetCRDT.class, null);

        // Create registration event for user
        Message newEvt = new Message(fullName + " has registered!", loginName, date);
        writeMessage(txn, newEvt, newUser.eventList, null);

        return newUser;
    }

    void updateUser(boolean status, String fullName, long birthday) {
        logger.info("Update user data for " + this.currentUser.loginName);
        this.currentUser.active = status;
        this.currentUser.fullName = fullName;
        this.currentUser.birthday = birthday;
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            LWWRegisterCRDT<User> reg = get(txn, NamingScheme.forUser(this.currentUser.loginName), true,
                    LWWRegisterCRDT.class, updatesSubscriber);
            reg.set((User) currentUser.copy());
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public User read(final String name, final Collection<Message> msgs, final Collection<Message> evnts) {
        logger.info("Get site report for " + name);
        TxnHandle txn = null;
        User user = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);
            LWWRegisterCRDT<User> reg = (LWWRegisterCRDT<User>) get(txn, NamingScheme.forUser(name), false,
                    LWWRegisterCRDT.class);
            user = reg.getValue();

            bulkGet(txn, user.msgList, user.eventList);

            msgs.addAll((get(txn, user.msgList, false, AddWinsSetCRDT.class, updatesSubscriber)).getValue());
            evnts.addAll((get(txn, user.eventList, false, AddWinsSetCRDT.class, updatesSubscriber)).getValue());
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return user;
    }

    // FIXME return error code?
    @SuppressWarnings("unchecked")
    public void postMessage(String receiverName, String msg, long date) {
        logger.info("Post status msg from " + this.currentUser.loginName + " for " + receiverName);
        Message newMsg = new Message(msg, this.currentUser.loginName, date);
        Message newEvt = new Message(currentUser.loginName + " has posted a message  to " + receiverName,
                this.currentUser.loginName, date);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            User receiver = ((LWWRegisterCRDT<User>) get(txn, NamingScheme.forUser(receiverName), false,
                    LWWRegisterCRDT.class)).getValue();

            bulkGet(txn, receiver.msgList, currentUser.eventList);

            writeMessage(txn, newMsg, receiver.msgList, updatesSubscriber);
            writeMessage(txn, newEvt, currentUser.eventList, updatesSubscriber);
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    public void updateStatus(String msg, long date) {
        logger.info("Update status for " + this.currentUser.loginName);
        Message newMsg = new Message(msg, this.currentUser.loginName, date);
        Message newEvt = new Message(currentUser.loginName + " has an updated status", this.currentUser.loginName, date);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);

            bulkGet(txn, currentUser.msgList, currentUser.eventList);

            writeMessage(txn, newMsg, currentUser.msgList, updatesSubscriber);
            writeMessage(txn, newEvt, currentUser.eventList, updatesSubscriber);
            commitTxn(txn);
            // TODO Broadcast update to friends
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    void answerFriendRequest(String requester, boolean accept) {
        logger.info("Answered friend request from " + this.currentUser.loginName + " for " + requester);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            // Obtain data of requesting user
            User other = ((LWWRegisterCRDT<User>) get(txn, NamingScheme.forUser(requester), false,
                    LWWRegisterCRDT.class)).getValue();

            bulkGet(txn, currentUser.inFriendReq, other.outFriendReq);

            // Remove information for request
            AddWinsSetCRDT inFriendReq = get(txn, currentUser.inFriendReq, false, AddWinsSetCRDT.class,
                    updatesSubscriber);
            inFriendReq.remove(NamingScheme.forUser(requester));
            AddWinsSetCRDT outFriendReq = get(txn, other.outFriendReq, false, AddWinsSetCRDT.class, updatesSubscriber);
            outFriendReq.remove(NamingScheme.forUser(this.currentUser.loginName));

            // Befriend if accepted
            if (accept) {
                bulkGet(txn, currentUser.friendList, other.friendList);

                AddWinsSetCRDT friends = get(txn, currentUser.friendList, false, AddWinsSetCRDT.class,
                        updatesSubscriber);
                friends.add(NamingScheme.forUser(requester));
                AddWinsSetCRDT requesterFriends = get(txn, other.friendList, false, AddWinsSetCRDT.class,
                        updatesSubscriber);
                requesterFriends.add(NamingScheme.forUser(this.currentUser.loginName));
            }
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    void sendFriendRequest(String receiverName) {
        logger.info("Sending friend request from to " + receiverName);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            // Obtain data of friend
            User other = ((LWWRegisterCRDT<User>) get(txn, NamingScheme.forUser(receiverName), false,
                    LWWRegisterCRDT.class)).getValue();

            bulkGet(txn, other.inFriendReq, currentUser.outFriendReq);

            // Add data for request
            AddWinsSetCRDT inFriendReq = get(txn, other.inFriendReq, false, AddWinsSetCRDT.class, updatesSubscriber);
            inFriendReq.add(NamingScheme.forUser(currentUser.loginName));
            AddWinsSetCRDT outFriendReq = get(txn, currentUser.outFriendReq, false, AddWinsSetCRDT.class,
                    updatesSubscriber);
            outFriendReq.add(NamingScheme.forUser(receiverName));

            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    public void befriend(String receiverName) {
        logger.info("Befriending " + receiverName);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);

            bulkGet(txn, NamingScheme.forUser(receiverName), currentUser.friendList);

            // Obtain new friend's data
            User friend = ((LWWRegisterCRDT<User>) get(txn, NamingScheme.forUser(receiverName), false,
                    LWWRegisterCRDT.class)).getValue();

            // Register him as my friend
            AddWinsSetCRDT friends = get(txn, currentUser.friendList, false, AddWinsSetCRDT.class, updatesSubscriber);
            friends.add(NamingScheme.forUser(receiverName));

            // Register me as his friend
            AddWinsSetCRDT requesterFriends = get(txn, friend.friendList, false, AddWinsSetCRDT.class,
                    updatesSubscriber);
            requesterFriends.add(NamingScheme.forUser(this.currentUser.loginName));
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    public Set<Friend> readFriendList(String name) {
        logger.info("Get friends of " + name);
        Set<Friend> friends = new HashSet<Friend>();
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);
            // Obtain user data

            User user = ((LWWRegisterCRDT<User>) get(txn, NamingScheme.forUser(name), false, LWWRegisterCRDT.class,
                    updatesSubscriber)).getValue();

            Set<CRDTIdentifier> friendIds = get(txn, user.friendList, false, AddWinsSetCRDT.class, updatesSubscriber)
                    .getValue();

            bulkGet(txn, friendIds);

            for (CRDTIdentifier f : friendIds) {
                User u = ((LWWRegisterCRDT<User>) get(txn, f, false, LWWRegisterCRDT.class, updatesSubscriber))
                        .getValue();
                friends.add(new Friend(u.fullName, f));
            }
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return friends;
    }

    private void writeMessage(TxnHandle txn, Message msg, CRDTIdentifier set, ObjectUpdatesListener listener)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        AddWinsSetCRDT messages = txn.get(set, false, AddWinsSetCRDT.class, listener);
        messages.add(msg);
    }

    private void commitTxn(final TxnHandle txn) {
        if (asyncCommit) {
            txn.commitAsync(null);
        } else {
            txn.commit();
        }
    }

    Map<CRDTIdentifier, CRDT<?>> bulkRes = new HashMap<CRDTIdentifier, CRDT<?>>();

    void bulkGet(TxnHandle txn, CRDTIdentifier... ids) {
        txn.bulkGet(ids);
    }

    void bulkGet(TxnHandle txn, Set<CRDTIdentifier> ids) {
        txn.bulkGet(ids.toArray(new CRDTIdentifier[ids.size()]));
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> V get(TxnHandle txn, CRDTIdentifier id, boolean create, Class<V> classOfV,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        V res = (V) bulkRes.remove(id);
        if (res == null)
            res = txn.get(id, create, classOfV, updatesListener);
        return res;
    }

    Tally getLatency = new Tally("GetLatency");

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>, T extends CRDT<V>> T get(TxnHandle txn, CRDTIdentifier id, boolean create, Class<V> classOfT)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {

        T res = (T) bulkRes.remove(id);
        if (res == null)
            res = (T) txn.get(id, create, classOfT);

        return res;
    }
}

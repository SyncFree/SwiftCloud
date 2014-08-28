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

import swift.crdt.AddWinsIdSetCRDT;
import swift.crdt.AddWinsMessageSetCRDT;
import swift.crdt.AddWinsSetCRDT;
import swift.crdt.BloatedIntegerCRDT;
import swift.crdt.IntegerCRDT;
import swift.crdt.LWWUserRegisterCRDT;
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
import swift.utils.SafeLog;
import swift.utils.SafeLog.ReportType;
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
    private final boolean bloatedCounters;

    public SwiftSocialOps(SwiftSession clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean subscribeUpdates, boolean asyncCommit, boolean bloatedCounters) {
        server = clientServer;
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.updatesSubscriber = subscribeUpdates ? TxnHandle.UPDATES_SUBSCRIBER : null;
        this.asyncCommit = asyncCommit;
        this.bloatedCounters = bloatedCounters;
    }

    public SwiftSession getSwift() {
        return server;
    }

    // FIXME Return type integer encoding error msg?
    public boolean login(String loginName, String passwd) throws SwiftException {
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
            User user = txn.get(NamingScheme.forUser(loginName), false, LWWUserRegisterCRDT.class, updatesSubscriber)
                    .getValue();

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
            throw e;
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            throw e;
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

        LWWUserRegisterCRDT reg = txn.get(NamingScheme.forUser(loginName), true, LWWUserRegisterCRDT.class, null);

        User newUser = new User(loginName, passwd, fullName, birthday, true);
        reg.set((User) newUser.copy());

        // Construct the associated sets with messages, friends etc.
        txn.get(newUser.msgList, true, AddWinsMessageSetCRDT.class, null);
        txn.get(newUser.eventList, true, AddWinsMessageSetCRDT.class, null);
        txn.get(newUser.friendList, true, AddWinsIdSetCRDT.class, null);
        txn.get(newUser.inFriendReq, true, AddWinsIdSetCRDT.class, null);
        txn.get(newUser.outFriendReq, true, AddWinsIdSetCRDT.class, null);
        if (bloatedCounters) {
            txn.get(newUser.viewsCounter, true, BloatedIntegerCRDT.class, null);
        } else {
            txn.get(newUser.viewsCounter, true, IntegerCRDT.class, null);
        }

        // Create registration event for user
        Message newEvt = new Message(fullName + " has registered!", loginName, date);
        writeMessage(txn, newEvt, newUser.eventList, null);

        return newUser;
    }

    @SuppressWarnings("unchecked")
    void updateUser(boolean status, String fullName, long birthday) throws SwiftException {
        logger.info("Update user data for " + this.currentUser.loginName);
        this.currentUser.active = status;
        this.currentUser.fullName = fullName;
        this.currentUser.birthday = birthday;
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            LWWUserRegisterCRDT reg = get(txn, NamingScheme.forUser(this.currentUser.loginName), true,
                    LWWUserRegisterCRDT.class, updatesSubscriber);
            reg.set((User) currentUser.copy());
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            throw e;
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public int read(final String name, final Collection<Message> msgs, final Collection<Message> evnts,
            boolean readPageViewsCounter) throws SwiftException {
        logger.info("Get site report for " + name);
        TxnHandle txn = null;
        User user = null;
        int currentPageViews = 0;
        try {
            final long startTimestamp = System.currentTimeMillis();
            final boolean readOnly = !readPageViewsCounter;
            txn = server.beginTxn(isolationLevel, cachePolicy, readOnly);
            LWWUserRegisterCRDT reg = get(txn, NamingScheme.forUser(name), false, LWWUserRegisterCRDT.class);
            user = reg.getValue();

            bulkGet(txn, user.msgList, user.eventList, user.viewsCounter);

            msgs.addAll((get(txn, user.msgList, false, AddWinsMessageSetCRDT.class, updatesSubscriber)).getValue());
            evnts.addAll((get(txn, user.eventList, false, AddWinsMessageSetCRDT.class, updatesSubscriber)).getValue());
            if (readPageViewsCounter) {
                if (bloatedCounters) {
                    final BloatedIntegerCRDT pageViewsCounter = get(txn, user.viewsCounter, false,
                            BloatedIntegerCRDT.class, updatesSubscriber);
                    currentPageViews = pageViewsCounter.getValue();
                    pageViewsCounter.add(1);
                } else {
                    final IntegerCRDT pageViewsCounter = get(txn, user.viewsCounter, false, IntegerCRDT.class,
                            updatesSubscriber);
                    currentPageViews = pageViewsCounter.getValue();
                    pageViewsCounter.add(1);
                }
            }
            commitTxn(txn);

            // scoutid,key,size
            SafeLog.report(ReportType.STALENESS_READ, server.getScout().getScoutId(), user.msgList, msgs.size());
            // ((ArrayList<Message>)msgs).get(0).getDate());
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            throw e;
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return currentPageViews;
    }

    // FIXME return error code?
    @SuppressWarnings("unchecked")
    public void postMessage(String receiverName, String msg, long date) throws SwiftException {
        logger.info("Post status msg from " + this.currentUser.loginName + " for " + receiverName);
        Message newMsg = new Message(msg, this.currentUser.loginName, date);
        Message newEvt = new Message(currentUser.loginName + " has posted a message  to " + receiverName,
                this.currentUser.loginName, date);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            User receiver = get(txn, NamingScheme.forUser(receiverName), false, LWWUserRegisterCRDT.class).getValue();

            bulkGet(txn, receiver.msgList, currentUser.eventList);

            writeMessage(txn, newMsg, receiver.msgList, updatesSubscriber);
            writeMessage(txn, newEvt, currentUser.eventList, updatesSubscriber);
            commitTxn(txn);
            // scoutid,key
            SafeLog.report(ReportType.STALENESS_WRITE, server.getScout().getScoutId(), receiver.msgList);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            throw e;
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    public void updateStatus(String msg, long date) throws SwiftException {
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

            // scoutid,key
            SafeLog.report(ReportType.STALENESS_WRITE, server.getScout().getScoutId(), currentUser.msgList);
            // TODO Broadcast update to friends
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            throw e;
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
            User other = get(txn, NamingScheme.forUser(requester), false, LWWUserRegisterCRDT.class).getValue();

            bulkGet(txn, currentUser.inFriendReq, other.outFriendReq);

            // Remove information for request
            AddWinsIdSetCRDT inFriendReq = get(txn, currentUser.inFriendReq, false, AddWinsIdSetCRDT.class,
                    updatesSubscriber);
            inFriendReq.remove(NamingScheme.forUser(requester));
            AddWinsIdSetCRDT outFriendReq = get(txn, other.outFriendReq, false, AddWinsIdSetCRDT.class,
                    updatesSubscriber);
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

    void sendFriendRequest(String receiverName) throws SwiftException {
        logger.info("Sending friend request from to " + receiverName);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            // Obtain data of friend
            User other = get(txn, NamingScheme.forUser(receiverName), false, LWWUserRegisterCRDT.class).getValue();

            bulkGet(txn, other.inFriendReq, currentUser.outFriendReq);

            // Add data for request
            AddWinsIdSetCRDT inFriendReq = get(txn, other.inFriendReq, false, AddWinsIdSetCRDT.class, updatesSubscriber);
            inFriendReq.add(NamingScheme.forUser(currentUser.loginName));
            AddWinsIdSetCRDT outFriendReq = get(txn, currentUser.outFriendReq, false, AddWinsIdSetCRDT.class,
                    updatesSubscriber);
            outFriendReq.add(NamingScheme.forUser(receiverName));

            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            throw e;
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    public void befriend(String receiverName) throws SwiftException {
        logger.info("Befriending " + receiverName);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);

            bulkGet(txn, NamingScheme.forUser(receiverName), currentUser.friendList);

            // Obtain new friend's data
            User friend = get(txn, NamingScheme.forUser(receiverName), false, LWWUserRegisterCRDT.class).getValue();

            // Register him as my friend
            AddWinsIdSetCRDT friends = get(txn, currentUser.friendList, false, AddWinsIdSetCRDT.class,
                    updatesSubscriber);
            friends.add(NamingScheme.forUser(receiverName));

            // Register me as his friend
            AddWinsIdSetCRDT requesterFriends = get(txn, friend.friendList, false, AddWinsIdSetCRDT.class,
                    updatesSubscriber);
            requesterFriends.add(NamingScheme.forUser(this.currentUser.loginName));
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            throw e;
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    public Set<Friend> readFriendList(String name) throws SwiftException {
        logger.info("Get friends of " + name);
        Set<Friend> friends = new HashSet<Friend>();
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, true);
            // Obtain user data

            User user = get(txn, NamingScheme.forUser(name), false, LWWUserRegisterCRDT.class, updatesSubscriber)
                    .getValue();

            Set<CRDTIdentifier> friendIds = get(txn, user.friendList, false, AddWinsIdSetCRDT.class, updatesSubscriber)
                    .getValue();

            bulkGet(txn, friendIds);

            for (CRDTIdentifier f : friendIds) {
                User u = get(txn, f, false, LWWUserRegisterCRDT.class, updatesSubscriber).getValue();
                friends.add(new Friend(u.fullName, f));
            }
            commitTxn(txn);
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
            throw e;
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return friends;
    }

    private void writeMessage(TxnHandle txn, Message msg, CRDTIdentifier set, ObjectUpdatesListener listener)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        AddWinsMessageSetCRDT messages = txn.get(set, false, AddWinsMessageSetCRDT.class, listener);
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
        txn.bulkGet(updatesSubscriber != null, ids);
    }

    void bulkGet(TxnHandle txn, Set<CRDTIdentifier> ids) {
        txn.bulkGet(updatesSubscriber != null, ids.toArray(new CRDTIdentifier[ids.size()]));
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
    <V extends CRDT<V>> V get(TxnHandle txn, CRDTIdentifier id, boolean create, Class<V> classOfT)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {

        V res = (V) bulkRes.remove(id);
        if (res == null)
            res = (V) txn.get(id, create, classOfT);

        return res;
    }
}

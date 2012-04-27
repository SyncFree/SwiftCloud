package swift.application.social;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.crdt.CRDTIdentifier;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.SetIds;
import swift.crdt.SetMsg;
import swift.crdt.SetTxnLocalId;
import swift.crdt.SetTxnLocalMsg;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

// implements the social network functionality
// see wsocial_srv.h

public class SwiftSocial {

    private static Logger logger = Logger.getLogger("swift.social");
    {
        logger.setLevel(Level.INFO);
    }
    // FIXME Add sessions? Local login possible? Cookies?
    private User currentUser;
    private Swift server;

    public SwiftSocial(Swift clientServer) {
        server = clientServer;
    }

    // FIXME Return type integer encoding error msg?
    boolean login(String loginName, String passwd) {
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
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true);
            @SuppressWarnings("unchecked")
            User user = (User) (txn.get(NamingScheme.forUser(loginName), false, RegisterVersioned.class)).getValue();

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
                    txn.commitAsync(null);
                    return true;
                } else {
                    logger.info("Wrong password for " + loginName);
                }
            } else {
                logger.info("User has not been registered " + loginName);
            }
        } catch (NetworkException e) {
            e.printStackTrace();
        } catch (WrongTypeException e) {
            // should not happen
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            logger.info("User " + loginName + " is not known");
        } catch (VersionNotFoundException e) {
            // should not happen
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }

        return false;
    }

    void logout(String loginName) {
        currentUser = null;
        // FIXME End session? handle cookies?
        logger.info(loginName + " successfully logged out");
    }

    // FIXME Return error code?
    @SuppressWarnings("unchecked")
    void registerUser(final String loginName, final String passwd, final String fullName, final long birthday,
            final long date) {
        logger.info("Got registration request for " + loginName);
        // FIXME How do we guarantee unique login names?
        // WalterSocial suggests using dedicated (non-replicated) login server.
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(loginName), true,
                    RegisterVersioned.class);
            txn.get(NamingScheme.forMessages(loginName), true, SetMsg.class);
            txn.get(NamingScheme.forEvents(loginName), true, SetMsg.class);
            txn.get(NamingScheme.forFriends(loginName), true, SetIds.class);
            txn.get(NamingScheme.forInFriendReq(loginName), true, SetIds.class);
            txn.get(NamingScheme.forOutFriendReq(loginName), true, SetIds.class);

            User newUser = new User(loginName, passwd, fullName, birthday, true);
            reg.set(newUser);
            Message newEvt = new Message(fullName + " has registered!", loginName, date);
            writeMessage(txn, newEvt, NamingScheme.forEvents(loginName));
            logger.info("Registered user: " + newUser);
            txn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    void updateUser(boolean status, String fullName, long birthday, int maritalStatus) {
        logger.info("Update user data for " + this.currentUser.loginName);
        this.currentUser.active = status;
        this.currentUser.fullName = fullName;
        this.currentUser.birthday = birthday;
        this.currentUser.maritalStatus = maritalStatus;
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(
                    NamingScheme.forUser(this.currentUser.loginName), true, RegisterVersioned.class);
            reg.set(currentUser);
            txn.commitAsync(null);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    @SuppressWarnings("unchecked")
    User read(final String name, final Collection<Message> msgs, final Collection<Message> evnts) {
        logger.info("Get site report for " + name);
        TxnHandle txn = null;
        User user = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, true);
            RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(name), false,
                    RegisterVersioned.class);
            user = reg.getValue();
            msgs.addAll(((SetTxnLocalMsg) txn.get(NamingScheme.forMessages(name), false, SetMsg.class)).getValue());
            evnts.addAll(((SetTxnLocalMsg) txn.get(NamingScheme.forEvents(name), false, SetMsg.class)).getValue());
            txn.commitAsync(null);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return user;
    }

    // FIXME return error code?
    void postMessage(String receiverName, String msg, long date) {
        logger.info("Post status msg from " + this.currentUser.loginName + " for " + receiverName);
        Message newMsg = new Message(msg, this.currentUser.loginName, date);
        Message newEvt = new Message(currentUser.loginName + " has posted a message  to " + receiverName,
                this.currentUser.loginName, date);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            writeMessage(txn, newMsg, NamingScheme.forMessages(receiverName));
            writeMessage(txn, newEvt, NamingScheme.forEvents(currentUser.loginName));
            // TODO Use stored Ids
            txn.commitAsync(null);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    void updateStatus(String msg, long date) {
        logger.info("Update status for " + this.currentUser.loginName);
        Message newMsg = new Message(msg, this.currentUser.loginName, date);
        Message newEvt = new Message(currentUser.loginName + " has an updated status", this.currentUser.loginName, date);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            writeMessage(txn, newMsg, NamingScheme.forMessages(currentUser.loginName));
            writeMessage(txn, newEvt, NamingScheme.forEvents(currentUser.loginName));
            // TODO Use stored Ids
            txn.commitAsync(null);
            // TODO Broadcast update to friends
        } catch (Exception e) {
            e.printStackTrace();
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
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            SetTxnLocalId inFriendReq = (SetTxnLocalId) txn.get(
                    NamingScheme.forInFriendReq(this.currentUser.loginName), false, SetIds.class);
            inFriendReq.remove(NamingScheme.forUser(requester));
            SetTxnLocalId outFriendReq = (SetTxnLocalId) txn.get(NamingScheme.forOutFriendReq(requester), false,
                    SetIds.class);
            outFriendReq.remove(NamingScheme.forUser(this.currentUser.loginName));
            if (accept) {
                SetTxnLocalId friends = (SetTxnLocalId) txn.get(NamingScheme.forFriends(this.currentUser.loginName),
                        false, SetIds.class);
                friends.insert(NamingScheme.forUser(requester));
                SetTxnLocalId requesterFriends = (SetTxnLocalId) txn.get(NamingScheme.forFriends(requester), false,
                        SetIds.class);
                requesterFriends.insert(NamingScheme.forUser(this.currentUser.loginName));
                txn.commitAsync(null);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            SetTxnLocalId inFriendReq = (SetTxnLocalId) txn.get(NamingScheme.forInFriendReq(receiverName), false,
                    SetIds.class);
            inFriendReq.insert(NamingScheme.forUser(this.currentUser.loginName));
            SetTxnLocalId outFriendReq = (SetTxnLocalId) txn.get(
                    NamingScheme.forOutFriendReq(this.currentUser.loginName), false, SetIds.class);
            outFriendReq.remove(NamingScheme.forUser(this.currentUser.loginName));
            txn.commitAsync(null);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    void befriend(String receiverName) {
        logger.info("Befriending " + receiverName);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.MOST_RECENT, false);
            SetTxnLocalId friends = (SetTxnLocalId) txn.get(NamingScheme.forFriends(this.currentUser.loginName), false,
                    SetIds.class);
            friends.insert(NamingScheme.forUser(receiverName));
            SetTxnLocalId requesterFriends = (SetTxnLocalId) txn.get(NamingScheme.forFriends(receiverName), false,
                    SetIds.class);
            requesterFriends.insert(NamingScheme.forUser(this.currentUser.loginName));
            txn.commitAsync(null);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    Set<Friend> readFriendList(String name) {
        logger.info("Get friends of " + name);
        Set<Friend> friends = new HashSet<Friend>();
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, true);
            Set<CRDTIdentifier> friendIds = ((SetTxnLocalId) txn
                    .get(NamingScheme.forFriends(name), false, SetIds.class)).getValue();
            for (CRDTIdentifier f : friendIds) {
                User u = ((RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(name), false, RegisterVersioned.class))
                        .getValue();
                friends.add(new Friend(u.fullName, f));
            }
            txn.commitAsync(null);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return friends;
    }

    private void writeMessage(TxnHandle txn, Message msg, CRDTIdentifier set) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        SetTxnLocalMsg messages = (SetTxnLocalMsg) txn.get(set, false, SetMsg.class);
        messages.insert(msg);
    }

}

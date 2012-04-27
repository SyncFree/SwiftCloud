package swift.application.social;

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

        try {
            // Check if user is known at all
            // FIXME Is login possible in offline mode?

            TxnHandle txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true);
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
    void registerUser(String loginName, String passwd, String fullName, long birthday) {
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
            logger.info("Registered user: " + newUser);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commitAsync(null);
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commitAsync(null);
            }
        }
    }

    @SuppressWarnings("unchecked")
    User read(final String name, final Set<Message> messages, final Set<Message> events) {
        logger.info("Get site report for " + name);
        TxnHandle txn = null;
        User user = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, true);
            RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(name), false,
                    RegisterVersioned.class);
            user = reg.getValue();
            messages.addAll(((SetTxnLocalMsg) txn.get(NamingScheme.forMessages(name), false, SetMsg.class)).getValue());
            events.addAll(((SetTxnLocalMsg) txn.get(NamingScheme.forEvents(name), false, SetMsg.class)).getValue());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commitAsync(null);
            }
        }
        return user;
    }

    // FIXME return error code?
    void postMessage(String receiverName, String msg, long date) {
        logger.info("Post status msg from " + this.currentUser.loginName + " for " + receiverName);
        Message newMsg = new Message(msg, this.currentUser.loginName, date);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            SetTxnLocalMsg messages = (SetTxnLocalMsg) txn.get(NamingScheme.forMessages(receiverName), false,
                    SetMsg.class);
            messages.insert(newMsg);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commitAsync(null);
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
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commitAsync(null);
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commitAsync(null);
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commitAsync(null);
            }
        }
        return friends;
    }
}

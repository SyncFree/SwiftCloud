package swift.application.social;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.SetMsg;
import swift.crdt.SetStrings;
import swift.crdt.SetTxnLocalMsg;
import swift.crdt.SetTxnLocalString;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

// implements the social network functionality
// see wsocial_srv.h

public class SwiftSocial {

    protected static Logger logger = Logger.getLogger("swift.social");
    private User currentUser;
    private Swift server;

    public SwiftSocial(Swift clientServer) {
        server = clientServer;
    }

    // FIXME Return type integer encoding error msg?
    boolean login(String loginName, String passwd) {
        logger.info("Got login request from user " + loginName);

        // Check if user is already logged in
        if (currentUser != null && loginName.equals(currentUser)) {
            logger.info(loginName + " is already logged in");
            return true;
        }

        // Check if user is known at all
        TxnHandle txn = null;
        // FIXME Is login possible in offline mode?
        User user;
        boolean result = false;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true);
            user = (User) (txn.get(NamingScheme.forLogin(loginName), false, RegisterVersioned.class)).getValue();
            // Check password
            // FIXME We actually need an external authentification mechanism, as
            // clients cannot be trusted.
            // In Walter, authentification is done on server side, within the
            // data center. Moving password (even if hashed) to the client is a
            // security breach.
            if (!user.password.equals(passwd)) {
                logger.info("Wrong password for " + loginName);
            } else {
                // FIXME Add sessions? Local login possible? Cookies?
                currentUser = user;
                logger.info(loginName + " successfully logged in");
                result = true;
            }
        } catch (NetworkException e) {
            e.printStackTrace();
            result = false;
        } catch (WrongTypeException e) {
            // should not happen
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            logger.info("User " + loginName + " is not known");
        } catch (VersionNotFoundException e) {
            // should not happen
            e.printStackTrace();
        } finally {
            txn.commit();
        }
        return result;
    }

    void logout(String loginName) {
        currentUser = null;
        // FIXME End session? handle cookies?
        logger.info(loginName + " successfully logged out");
    }

    // FIXME Return error code?
    void addUser(String loginName, String passwd) {
        logger.info("Got registration request for " + loginName);
        // FIXME How do we guarantee unique login names?
        // WalterSocial suggests using dedicated (non-replicated) login server.
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(NamingScheme.forLogin(loginName), true,
                    RegisterVersioned.class);
            txn.get(NamingScheme.forMessages(loginName), true, SetMsg.class);
            //preguica: either create objects here or set true when accessing them in other methods
            txn.get(NamingScheme.forFriends(loginName), true, SetStrings.class);
            txn.get(NamingScheme.forInFriendReq(loginName), true, SetStrings.class);
            txn.get(NamingScheme.forOutFriendReq(loginName), true, SetStrings.class);
            User newUser = new User(loginName, passwd);
            reg.set(newUser);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commit();
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
                    NamingScheme.forLogin(this.currentUser.loginName), true, RegisterVersioned.class);
            reg.set(currentUser);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commit();
            }
        }
    }

    Set<Message> getSiteReport() {
        logger.info("Get site report for " + this.currentUser.loginName);
        Set<Message> postings = new HashSet<Message>();
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, true);
            SetTxnLocalMsg messages = (SetTxnLocalMsg) txn.get(NamingScheme.forMessages(this.currentUser.loginName),
                    false, SetMsg.class);
            postings = messages.getValue();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commit();
            }
        }
        return postings;

    }

    // FIXME return error code?
    void postMessage(String receiverName, String msg, long date) {
        logger.info("Post status msg from " + this.currentUser.loginName + " for " + receiverName);
        Message newMsg = new Message(msg, this.currentUser.loginName, receiverName, date);
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
                txn.commit();
            }
        }
    }

    void answerFriendRequest(String requester, boolean accept) {
        logger.info("Answered friend request from " + this.currentUser.loginName + " for " + requester);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            SetTxnLocalString inFriendReq = (SetTxnLocalString) txn.get(
                    NamingScheme.forInFriendReq(this.currentUser.loginName), false, SetStrings.class);
            inFriendReq.remove(requester);
            if (accept) {
                SetTxnLocalString friends = (SetTxnLocalString) txn.get(
                        NamingScheme.forFriends(this.currentUser.loginName), false, SetStrings.class);
                friends.insert(requester);
            }
            SetTxnLocalString outFriendReq = (SetTxnLocalString) txn.get(NamingScheme.forOutFriendReq(requester),
                    false, SetStrings.class);
            outFriendReq.remove(this.currentUser.loginName);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commit();
            }
        }
    }

    void sendFriendRequest(String receiverName) {
        logger.info("Sending friend request from to " + receiverName);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            SetTxnLocalString inFriendReq = (SetTxnLocalString) txn.get(NamingScheme.forInFriendReq(receiverName),
                    false, SetStrings.class);
            inFriendReq.insert(this.currentUser.loginName);
            SetTxnLocalString outFriendReq = (SetTxnLocalString) txn.get(
                    NamingScheme.forOutFriendReq(this.currentUser.loginName), false, SetStrings.class);
            outFriendReq.remove(this.currentUser.loginName);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commit();
            }
        }
    }

    Set<String> readUserFriends() {
        logger.info("Get friends for " + this.currentUser.loginName);
        Set<String> friendNames = new HashSet<String>();
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, true);
            SetTxnLocalString friends = (SetTxnLocalString) txn.get(
                    NamingScheme.forFriends(this.currentUser.loginName), false, SetStrings.class);
            friendNames = friends.getValue();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null) {
                txn.commit();
            }
        }
        return friendNames;
    }

}

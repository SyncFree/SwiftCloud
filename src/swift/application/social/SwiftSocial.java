package swift.application.social;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.SetMsg;
import swift.crdt.SetTxnLocalMsg;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
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
        if (currentUser != null && loginName.equals(currentUser.loginName)) {
            logger.info(loginName + " is already logged in");
            return true;
        }

        // Check if user is known at all
        TxnHandle txn = server.beginTxn(CachePolicy.STRICTLY_MOST_RECENT, true);
        // FIXME Is login possible in offline mode?
        User user;
        boolean result;
        try {
            user = (User) (txn.get(NamingScheme.forLogin(loginName), false, RegisterVersioned.class)).getValue();
            // Check password
            // FIXME We actually need an external authentification mechanism, as
            // clients cannot be trusted.
            // In Walter, authentification is done on server side, within the
            // data center. Moving password (even if hashed) to the client is a
            // security breach.
            if (!user.password.equals(passwd)) {
                logger.info("Wrong password for " + loginName);
                result = false;
            }
            // FIXME Add sessions? Local login possible? Cookies?
            currentUser = user;
            logger.info(loginName + " successfully logged in");
            result = true;
        } catch (WrongTypeException e) {
            // should not happen
            e.printStackTrace();
            result = false;
        } catch (NoSuchObjectException e) {
            logger.info("User " + loginName + " is not known");
            result = false;
        } catch (ConsistentSnapshotVersionNotFoundException e) {
            // should not happen
            e.printStackTrace();
            result = false;
        }
        txn.commit(false);
        return result;
    }

    void logout() {
        currentUser = null;
        // FIXME End session? handle cookies?
        logger.info(currentUser + " successfully logged out");
    }

    // FIXME Return error code?
    void addUser(String loginName, String passwd) {
        logger.info("Got registration request for " + loginName);
        // FIXME How do we guarantee unique login names?
        // WalterSocial suggests using dedicated (non-replicated) login server.
        TxnHandle txn = server.beginTxn(CachePolicy.STRICTLY_MOST_RECENT, false);
        try {
            RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(NamingScheme.forLogin(loginName), true,
                    RegisterVersioned.class);
            txn.get(NamingScheme.forMessages(loginName), true, SetMsg.class);
            User newUser = new User(loginName, passwd);
            reg.set(newUser);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            txn.commit(true);
        }
    }

    void updateUser() {
        // TODO
    }

    Set<Message> getSiteReport(String loginName) {
        logger.info("Get site report for " + loginName);
        Set<Message> postings = new HashSet<Message>();
        TxnHandle txn = server.beginTxn(CachePolicy.CACHED, true);
        try {
            SetTxnLocalMsg messages = (SetTxnLocalMsg) txn
                    .get(NamingScheme.forMessages(loginName), false, SetMsg.class);
            postings = messages.getValue();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            txn.commit(false);
        }
        return postings;

    }

    // FIXME return error code?
    void postMessage(String senderName, String receiverName, String msg, long date) {
        logger.info("Post status msg from " + senderName + " for " + receiverName);
        Message newMsg = new Message(msg, senderName, receiverName, date);
        TxnHandle txn = server.beginTxn(CachePolicy.CACHED, false);
        try {
            SetTxnLocalMsg messages = (SetTxnLocalMsg) txn.get(NamingScheme.forMessages(receiverName), false,
                    SetMsg.class);
            messages.insert(newMsg);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            txn.commit(false);
        }
    }

    void answerFriendRequest() {
        // TODO
    }

    void sendFriendRequest() {
        // TODO
    }

    void readUserFriends() {
        // TODO
    }

}

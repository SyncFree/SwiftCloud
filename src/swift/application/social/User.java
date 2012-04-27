package swift.application.social;

import java.util.Date;

import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Copyable;

public class User implements Copyable {
    CRDTIdentifier userId;
    String loginName;
    String password;
    // has user been deleted?
    boolean active;

    String fullName;
    long birthday;
    // FIXME: Better format: Enum?
    int maritalStatus;
    // CRDTIdentifier eventList;
    CRDTIdentifier msgList;

    CRDTIdentifier friendList;
    CRDTIdentifier inFriendReq;
    CRDTIdentifier outFriendReq;

    // TODO Add photos?
    // CRDTIdentifier photoAlbumList;

    /** DO NOT USE: Empty constructor needed for Kryo */
    public User() {
    }

    public User(final String loginName, final String password, final String fullName, final long birthday,
            boolean active) {
        this.loginName = loginName;
        this.fullName = fullName;
        this.password = password;
        this.userId = NamingScheme.forUser(loginName);
        this.birthday = birthday;
        this.maritalStatus = 0;
        this.active = active;
        this.msgList = NamingScheme.forMessages(loginName);
        this.friendList = NamingScheme.forFriends(loginName);
        this.inFriendReq = NamingScheme.forInFriendReq(loginName);
        this.outFriendReq = NamingScheme.forOutFriendReq(loginName);

    }

    @Override
    public Object copy() {
        User copyObj = new User(loginName, password, fullName, birthday, active);
        return copyObj;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(loginName).append(", ");
        sb.append(password).append("; ");
        sb.append(fullName).append(", ");
        sb.append(new Date(birthday));
        return sb.toString();
    }
}

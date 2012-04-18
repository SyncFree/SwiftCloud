package swift.application.social;

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

    public User() {
    }

    public User(String loginName, String password) {
        this.loginName = loginName;
        this.password = password;
        this.userId = NamingScheme.forLogin(loginName);
        this.birthday = 0;
        this.maritalStatus = 0;
        this.active = true;
        this.msgList = NamingScheme.forMessages(loginName);
        this.friendList = NamingScheme.forFriends(loginName);
        this.inFriendReq = NamingScheme.forInFriendReq(loginName);
        this.outFriendReq = NamingScheme.forOutFriendReq(loginName);

    }

    @Override
    public Object copy() {
        User copyObj = new User(loginName, password);
        copyObj.birthday = birthday;
        copyObj.maritalStatus = maritalStatus;
        copyObj.active = active;
        return copyObj;
    }
}

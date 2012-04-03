package swift.application.social;

import java.io.Serializable;

import swift.crdt.CRDTIdentifier;

public class User implements Serializable {
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

    // CRDTIdentifier friendList;
    // CRDTIdentifier inFriendReq;
    // CRDTIdentifier outFriendReq;

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
        this.msgList = NamingScheme.forMessageList(loginName);
    }
}

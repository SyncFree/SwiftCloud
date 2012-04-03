package swift.application.social;

import swift.crdt.CRDTIdentifier;

public class User {
    CRDTIdentifier userId;
    String loginName;
    String password;
    // has user been deleted?
    boolean active;

    String fullName;
    long birthday;
    // FIXME: Better format: Enum?
    int maritalStatus;
    CRDTIdentifier eventList;
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
    }

    public static CRDTIdentifier getCRDTIdentifier(final String loginName) {
        // see wsocial_shared.cc for scheme used in walter, based on MD5 hashing
        return new CRDTIdentifier("userData", loginName);
    }
}

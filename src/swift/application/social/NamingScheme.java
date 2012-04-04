package swift.application.social;

import swift.crdt.CRDTIdentifier;

public class NamingScheme {

    public static CRDTIdentifier forLogin(final String loginName) {
        // see wsocial_shared.cc for scheme used in walter, based on MD5 hashing
        return new CRDTIdentifier("users", loginName);
    }

    public static CRDTIdentifier forMessages(final String loginName) {
        // see wsocial_shared.cc for scheme used in walter, based on MD5 hashing
        return new CRDTIdentifier("messages", loginName);
    }
}

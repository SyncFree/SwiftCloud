package swift.application.social;

import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Copyable;

public class Friend implements Copyable, java.io.Serializable {
    String name;
    CRDTIdentifier userId;

    /** DO NOT USE: Empty constructor required by Kryo */
    Friend() {
    }

    public Friend(final String name, final CRDTIdentifier userId) {
        this.name = name;
        this.userId = userId.clone();
    }

    @Override
    public Object copy() {
        return new Friend(this.name, userId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || !(obj instanceof Friend)) {
            return false;
        }
        Friend other = (Friend) obj;
        return this.name.equals(other.name) && this.userId.equals(other.userId);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = result * 37 + userId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return this.name;
    }

}

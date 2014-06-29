/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.application.social;

import java.util.Date;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.Copyable;

public class User implements Copyable, KryoSerializable {
    CRDTIdentifier userId;
    String loginName;
    String password;
    // has user been deleted?
    boolean active;

    String fullName;
    long birthday;
    CRDTIdentifier eventList;
    CRDTIdentifier msgList;

    CRDTIdentifier friendList;
    CRDTIdentifier inFriendReq;
    CRDTIdentifier outFriendReq;
    CRDTIdentifier viewsCounter;

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
        this.active = active;
        // FIXME: do we need these statically computed references??
        // Is it for compatibility with Walter?
        this.msgList = NamingScheme.forMessages(loginName);
        this.eventList = NamingScheme.forEvents(loginName);
        this.friendList = NamingScheme.forFriends(loginName);
        this.inFriendReq = NamingScheme.forInFriendReq(loginName);
        this.outFriendReq = NamingScheme.forOutFriendReq(loginName);
        this.viewsCounter = NamingScheme.forViewsCounter(loginName);
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

    public String userInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append(fullName).append(", born ");
        sb.append(new Date(birthday));
        return sb.toString();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        userId.write(kryo, output);
        output.writeString(loginName);
        output.writeString(password);
        output.writeBoolean(active);
        output.writeString(fullName);
        output.writeLong(birthday);
        eventList.write(kryo, output);
        msgList.write(kryo, output);
        friendList.write(kryo, output);
        inFriendReq.write(kryo, output);
        outFriendReq.write(kryo, output);
        viewsCounter.write(kryo, output);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        userId = new CRDTIdentifier();
        userId.read(kryo, input);
        loginName = input.readString();
        password = input.readString();
        active = input.readBoolean();
        fullName = input.readString();
        birthday = input.readLong();

        eventList = new CRDTIdentifier();
        eventList.read(kryo, input);
        msgList = new CRDTIdentifier();
        msgList.read(kryo, input);
        friendList = new CRDTIdentifier();
        friendList.read(kryo, input);
        inFriendReq = new CRDTIdentifier();
        inFriendReq.read(kryo, input);
        outFriendReq = new CRDTIdentifier();
        outFriendReq.read(kryo, input);
        viewsCounter = new CRDTIdentifier();
        viewsCounter.read(kryo, input);
    }

}

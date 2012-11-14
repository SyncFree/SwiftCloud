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
package swift.dc;

import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.crdt.CRDTIdentifier;
import swift.dc.*;
import swift.dc.proto.DHTExecCRDTReplyHandler;
import sys.dht.api.DHT;

/**
 * Result of an exec operation in a CRDT
 * @author preguica
 * 
 */
public class ExecCRDTResult {
    CRDTIdentifier id;
    boolean result;
    boolean hasNotification;
    boolean notificationOnly;
    ObjectSubscriptionInfo info;

    public ExecCRDTResult(boolean result) {
        this.result = result;
        this.notificationOnly = true;
        this.info = null;
        hasNotification = false;
    }

    public ExecCRDTResult(boolean result, CRDTIdentifier id, boolean notificationOnly, ObjectSubscriptionInfo info) {
        this.result = result;
        this.id = id;
        this.notificationOnly = notificationOnly;
        this.info = info;
        hasNotification = true;
    }

    /**
     * Needed for Kryo serialization
     */
    public ExecCRDTResult() {
    }

    public boolean isResult() {
        return result;
    }

    public boolean isNotificationOnly() {
        return notificationOnly;
    }

    public ObjectSubscriptionInfo getInfo() {
        return info;
    }

    public boolean hasNotification() {
        return hasNotification;
    }

    public CRDTIdentifier getId() {
        return id;
    }

}

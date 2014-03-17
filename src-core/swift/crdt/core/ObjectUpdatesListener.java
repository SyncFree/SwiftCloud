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
package swift.crdt.core;


/**
 * Listener interface defining notifications for application on object updates.
 * 
 * @author mzawirski
 */
public interface ObjectUpdatesListener {
    /**
     * Fired when external update has been observed on an object previously read
     * by the client transaction. This notification is fired at most once per
     * registered listener for an object read. Listener instances can be reused
     * between subsequent reads of an object, or between reads of different
     * objects.
     * 
     * @param txn
     *            transaction where the old value of object has been read
     * @param id
     *            identifier of an object that has been externally updated
     * @param previousValue
     *            previous value of an object
     */
    void onObjectUpdate(final TxnHandle txn, final CRDTIdentifier id, final CRDT<?> previousValue);

    // TODO: add shouldContinueSubscription() method to simplify GC?

    /**
     * @return true if this listener only forces object updates subscription and
     *         it is not interested in actual
     *         {@link #onObjectUpdate(TxnHandle, CRDTIdentifier, CRDT)}
     *         calls.
     */
    boolean isSubscriptionOnly();
}

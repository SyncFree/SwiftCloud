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
package swift.utils;

/**
 * Durable sequential log. Implementations must be thread-safe.
 * 
 * @author mzawirski
 */
public interface TransactionsLog {
    /**
     * Atomically stores provided entries.
     * 
     * @param transactionId
     *            id of the transactions writing
     * @param entry
     *            object to write in the log, possibly asynchronously
     */
    void writeEntry(final long transactionId, Object entry);

    /**
     * Flushes the log, so all {@link #writeEntry(Object)} are guaranteed to be
     * durable at this time.
     */
    void flush();

    /**
     * Closes the log.
     */
    void close();
}
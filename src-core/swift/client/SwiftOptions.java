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
package swift.client;

import java.util.Properties;

/**
 * Options for Swift scout instance.
 * 
 * @author mzawirski
 */
final public class SwiftOptions {
    public static final boolean DEFAULT_CONCURRENT_OPEN_TRANSACTIONS = false;
    public static final boolean DEFAULT_DISASTER_SAFE = false;
    public static final int DEFAULT_MAX_ASYNC_TRANSACTIONS_QUEUED = 50;
    public static final int DEFAULT_TIMEOUT_MILLIS = 20 * 1000;
    public static final int DEFAULT_DEADLINE_MILLIS = DEFAULT_TIMEOUT_MILLIS;
    public static final int DEFAULT_NOTIFICATION_TIMEOUT_MILLIS = 5 * 1000;

    public static final long DEFAULT_CACHE_EVICTION_MILLIS = 60 * 1000;
    public static final int DEFAULT_CACHE_SIZE = 100000;
    public static final int DEFAULT_MAX_COMMIT_BATCH_SIZE = 1;
    public static final String DEFAULT_LOG_FILENAME = null;
    public static final boolean DEFAULT_LOG_FLUSH_ON_COMMIT = false;

    public static final String DEFAULT_STATISTICS_DIR = "statistics";
    public static final boolean DEFAULT_OVERWRITE_STATISTICS_DIR = true;
    public static final boolean DEFAULT_ENABLE_STATISTICS = false;

    public static final boolean DEFAULT_CAUSAL_NOTIFICATIONS = true;

    private String serverHostname;
    private int serverPort;
    private boolean disasterSafe = DEFAULT_DISASTER_SAFE;
    private boolean concurrentOpenTransactions = DEFAULT_CONCURRENT_OPEN_TRANSACTIONS;
    private int maxAsyncTransactionsQueued = DEFAULT_MAX_ASYNC_TRANSACTIONS_QUEUED;
    private int timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    private int deadlineMillis = DEFAULT_DEADLINE_MILLIS;
    private long cacheEvictionTimeMillis = DEFAULT_CACHE_EVICTION_MILLIS;
    private int cacheSize = DEFAULT_CACHE_SIZE;
    private int notificationTimeoutMillis = DEFAULT_NOTIFICATION_TIMEOUT_MILLIS;
    private int maxCommitBatchSize = DEFAULT_MAX_COMMIT_BATCH_SIZE;
    private String logFilename = DEFAULT_LOG_FILENAME;
    private boolean logFlushOnCommit = DEFAULT_LOG_FLUSH_ON_COMMIT;
    private boolean causalNotifications = DEFAULT_CAUSAL_NOTIFICATIONS;

    private boolean enableStatistics = DEFAULT_ENABLE_STATISTICS;
    private boolean overwriteStatisticsDir = DEFAULT_OVERWRITE_STATISTICS_DIR;
    private String statisticsOutputDir = DEFAULT_STATISTICS_DIR;

    // for kryo...
    SwiftOptions() {
    }

    /**
     * Creates a new instance with default options and provided server endpoint
     * 
     * @param serverHostname
     *            hostname of the store server
     * @param serverPort
     *            TCP port of the store server
     */
    public SwiftOptions(String serverHostname, int serverPort) {
        this(serverHostname, serverPort, new Properties());
    }

    /**
     * Creates a new instance with default values from the properties and
     * provided server endpoint
     * 
     * @param serverHostname
     *            hostname of the store server
     * @param serverPort
     *            TCP port of the store server
     */
    public SwiftOptions(String serverHostname, int serverPort, Properties defaultValues) {
        this.serverHostname = serverHostname;
        this.serverPort = serverPort;

        final String disasterSafeString = defaultValues.getProperty("swift.disasterSafe");
        if (disasterSafeString != null) {
            this.disasterSafe = Boolean.parseBoolean(disasterSafeString);
        }
        final String concurrentOpenTransactionsString = defaultValues.getProperty("swift.concurrentOpenTransactions");
        if (concurrentOpenTransactionsString != null) {
            this.concurrentOpenTransactions = Boolean.parseBoolean(concurrentOpenTransactionsString);
        }

        try {
            this.maxAsyncTransactionsQueued = Integer.parseInt(defaultValues
                    .getProperty("swift.maxAsyncTransactionsQueued"));
        } catch (NumberFormatException x) {
            // ignore
        }

        try {
            this.timeoutMillis = Integer.parseInt(defaultValues.getProperty("swift.timeoutMillis"));
        } catch (NumberFormatException x) {
            // ignore
        }
        try {
            this.deadlineMillis = Integer.parseInt(defaultValues.getProperty("swift.deadlineMillis"));
        } catch (NumberFormatException x) {
            // ignore
        }
        try {
            this.cacheEvictionTimeMillis = Integer.parseInt(defaultValues.getProperty("swift.cacheEvictionTimeMillis"));
        } catch (NumberFormatException x) {
            // ignore
        }
        try {
            this.cacheSize = Integer.parseInt(defaultValues.getProperty("swift.cacheSize"));
        } catch (NumberFormatException x) {
            // ignore
        }
        try {
            this.notificationTimeoutMillis = Integer.parseInt(defaultValues
                    .getProperty("swift.notificationTimeoutMillis"));
        } catch (NumberFormatException x) {
            // ignore
        }
        try {
            this.maxCommitBatchSize = Integer.parseInt(defaultValues.getProperty("swift.maxCommitBatchSize"));
        } catch (NumberFormatException x) {
            // ignore
        }

        if (defaultValues.getProperty("swift.logFilename") != null) {
            this.logFilename = defaultValues.getProperty("swift.logFilename");
        }
        final String logFlushOnCommitString = defaultValues.getProperty("swift.logFlushOnCommit");
        if (logFlushOnCommitString != null) {
            this.logFlushOnCommit = Boolean.parseBoolean(logFlushOnCommitString);
        }
        final String enableStatisticsString = defaultValues.getProperty("swift.enableStatistics");
        if (enableStatisticsString != null) {
            this.enableStatistics = Boolean.parseBoolean(enableStatisticsString);
        }
        if (defaultValues.getProperty("swift.statisticsOutputDir") != null) {
            this.statisticsOutputDir = defaultValues.getProperty("swift.statisticsOutputDir");
        }
        final String overwriteStatisticsDirString = defaultValues.getProperty("swift.overwriteStatisticsDir");
        if (overwriteStatisticsDirString != null) {
            this.overwriteStatisticsDir = Boolean.parseBoolean(overwriteStatisticsDirString);
        }
        final String causalNotificationsString = defaultValues.getProperty("swift.causalNotifications");
        if (causalNotificationsString != null) {
            this.causalNotifications = Boolean.parseBoolean(causalNotificationsString);
        }
    }

    /**
     * @return hostname of the store server
     */
    public String getServerHostname() {
        return serverHostname;
    }

    /**
     * @param serverHostname
     *            hostname of the store server
     */
    public void setServerHostname(String serverHostname) {
        this.serverHostname = serverHostname;
    }

    /**
     * @return TCP port of the store server
     */
    public int getServerPort() {
        return serverPort;
    }

    /**
     * @param serverPort
     *            TCP port of the store server
     */
    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    /**
     * @return true if only disaster safe committed (and local) transactions are
     *         read by transactions, so the client virtually never blocks due to
     *         system failures
     */
    public boolean isDisasterSafe() {
        return disasterSafe;
    }

    /**
     * @return true if the scout should assume notifications are delivered
     *         atomically in causal order for the cache as a whole.
     * 
     */
    public boolean assumeAtomicCausalNotifications() {
        return causalNotifications;
    }

    /**
     * @param disasterSafe
     *            when true, only disaster safe committed (and local)
     *            transactions are read by transactions, so the client virtually
     *            never blocks due to system failures
     */
    public void setDisasterSafe(boolean disasterSafe) {
        this.disasterSafe = disasterSafe;
    }

    public boolean isConcurrentOpenTransactions() {
        return concurrentOpenTransactions;
    }

    public void setConcurrentOpenTransactions(boolean concurrentOpenTransactions) {
        this.concurrentOpenTransactions = concurrentOpenTransactions;
    }

    /**
     * @return maximum number of asynchronous transactions queued before the
     *         client start to block application
     */
    public int getMaxAsyncTransactionsQueued() {
        return maxAsyncTransactionsQueued;
    }

    /**
     * @param maxAsyncQueuedTransactions
     *            maximum number of asynchronous transactions queued before the
     *            client start to block application
     */
    public void setMaxAsyncTransactionsQueued(int maxAsyncTransactionsQueued) {
        this.maxAsyncTransactionsQueued = maxAsyncTransactionsQueued;
    }

    /**
     * @return socket-level timeout for server replies in milliseconds
     */
    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    /**
     * @param timeoutMillis
     *            socket-level timeout for server replies in milliseconds
     */
    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * @return deadline for fulfilling user-triggered requests (get, refresh
     *         etc), total time possibly including retries
     */
    public int getDeadlineMillis() {
        return deadlineMillis;
    }

    /**
     * @param deadlineMillis
     *            deadline for fulfilling user-triggered requests (get, refresh
     *            etc), total time possibly including retries
     */
    public void setDeadlineMillis(int deadlineMillis) {
        this.deadlineMillis = deadlineMillis;
    }

    /**
     * @return eviction time for non-accessed objects in the cache
     */
    public long getCacheEvictionTimeMillis() {
        return cacheEvictionTimeMillis;
    }

    /**
     * @param cacheEvictionTimeMillis
     *            eviction time for non-accessed objects in the cache
     */
    public void setCacheEvictionTimeMillis(long cacheEvictionTimeMillis) {
        this.cacheEvictionTimeMillis = cacheEvictionTimeMillis;
    }

    /**
     * @return maximum number of objects in the cache
     */
    public int getCacheSize() {
        return cacheSize;
    }

    /**
     * @param cacheSize
     *            maximum number of objects in the cache
     */
    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    /**
     * @return timeout for notification requests if there are no changes to
     *         subscribed objects (in milliseconds)
     */
    public int getNotificationTimeoutMillis() {
        return notificationTimeoutMillis;
    }

    /**
     * @param notificationTimeoutMillis
     *            timeout for notification requests if there are no changes to
     *            subscribed objects (in milliseconds)
     */
    public void setNotificationTimeoutMillis(final int notificationTimeoutMillis) {
        this.notificationTimeoutMillis = notificationTimeoutMillis;
    }

    /**
     * @return maximum number of transactions in one commit request to the store
     */
    public int getMaxCommitBatchSize() {
        return maxCommitBatchSize;
    }

    /**
     * @param maxCommitBatchSize
     *            maximum number of transactions in one commit request to the
     *            store
     */
    public void setMaxCommitBatchSize(int maxCommitBatchSize) {
        this.maxCommitBatchSize = maxCommitBatchSize;
    }

    /**
     * @return filename used for durable log; null if log is not stored
     */
    public String getLogFilename() {
        return logFilename;
    }

    /**
     * @param logFilename
     *            filename used for durable log; null if log is not stored
     */
    public void setLogFilename(String logFilename) {
        this.logFilename = logFilename;
    }

    /**
     * @return true if log should be flushed on each local commit
     */
    public boolean isLogFlushOnCommit() {
        return logFlushOnCommit;
    }

    /**
     * @param logFlushOnCommit
     *            true if log should be flushed on each local commit
     */
    public void setLogFlushOnCommit(boolean logFlushOnCommit) {
        this.logFlushOnCommit = logFlushOnCommit;
    }

    /**
     * 
     * @param flag
     *            true if the scout should assume notifications are delivered
     *            atomically and in causal order for the whole cache.
     */
    public void setCausalNotifications(boolean flag) {
        this.causalNotifications = flag;
    }

    public void setStatisticsOutputFolder(String outputFolder, boolean overwriteExistingDir) {
        this.statisticsOutputDir = outputFolder;
        this.overwriteStatisticsDir = overwriteExistingDir;

    }

    public String getStatisticsOuputDir() {
        return statisticsOutputDir;
    }

    public boolean getStatisticsOverwriteDir() {
        return overwriteStatisticsDir;
    }

    public boolean isEnableStatistics() {
        return enableStatistics;
    }

    public void setEnableStatistics(boolean enableStatistics) {
        this.enableStatistics = enableStatistics;
    }
}

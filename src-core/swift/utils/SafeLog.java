package swift.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.EnumSet;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Shared logging class for safe reporting of predefined types of values from
 * concurrent threads.
 * 
 * @author mzawirski
 */
public class SafeLog {
    // TODO: alternatively, just inline these things as methods for type safety
    public static enum ReportType {
        STALENESS_READ(3, "scoutid,object,#msgs"),

        STALENESS_WRITE(2, "scoutid,object"),

        STALENESS_CALIB(5, "rtt_ms,skew_ms,scoutid,client_host,server_host"),

        METADATA(9, "timestamp_ms,session_id,message_name," + "total_message_size,version_or_update_size,value_size,"
                + "explicitly_computed_global_metadata,batch_size,max_vv_size,max_vv_exceptions_num"),

        APP_OP(3, "session_id,operation_name,duration_ms");

        private final int fieldsNumber;
        private final String semantics;

        private ReportType(int fieldsNumber, String semantics) {
            this.fieldsNumber = fieldsNumber;
            this.semantics = semantics;
        }

        public int getFieldsNumber() {
            return fieldsNumber;
        }

        public void printHeader() {
            printlnComment(String.format("report type %s formatted as timestamp_ms,%s,%s", name(), name(), semantics));
        }

        public boolean isEnabled() {
            return enabledReportsEnumSet.contains(this);
        }
    }

    public static final char COMMENT_CHAR = '#';
    private static Logger logger = Logger.getLogger(SafeLog.class.getName());
    private static EnumSet<ReportType> enabledReportsEnumSet = EnumSet.noneOf(ReportType.class);
    private static final BufferedWriter bufferedOutput = new BufferedWriter(new OutputStreamWriter(System.out));

    // TODO: fix potential synchronization race during init.
    public synchronized static void configureReportsFromProperties(Properties props) {
        String reports = props.getProperty("swift.reports");
        if (reports == null) {
            return;
        }
        for (final String report : reports.split(",")) {
            try {
                enabledReportsEnumSet.add(ReportType.valueOf(report));
                logger.info("Configured report " + report);
            } catch (IllegalArgumentException x) {
                logger.warning("Unrecognized report type " + report + " - ignoring");
            }
        }
    }

    public static void printHeader() {
        printlnComment("The log includes the following reports:");
        for (final ReportType type : enabledReportsEnumSet) {
            type.printHeader();
        }
    }

    public static void report(ReportType type, Object... args) {
        if (!type.isEnabled()) {
            return;
        }
        if (args.length != type.getFieldsNumber()) {
            logger.warning("Misformated report " + type + ": " + args);
        }
        synchronized (bufferedOutput) {
            try {
                bufferedOutput.write(Long.toString(System.currentTimeMillis()));
                bufferedOutput.write(',');
                bufferedOutput.write(type.name());
                for (final Object arg : args) {
                    bufferedOutput.write(',');
                    bufferedOutput.write(arg.toString());
                }
                bufferedOutput.write('\n');
            } catch (IOException e) {
                logger.warning("Cannot write to stdout: " + e);
            }
        }
    }

    public static void flush() {
        synchronized (bufferedOutput) {
            try {
                bufferedOutput.flush();
            } catch (IOException e) {
                logger.warning("Cannot flush stdout: " + e);
            }
        }
    }

    public static void printlnComment(String comment) {
        synchronized (bufferedOutput) {
            try {
                bufferedOutput.write(COMMENT_CHAR);
                bufferedOutput.write(comment);
                bufferedOutput.write('\n');
            } catch (IOException e) {
                logger.warning("Cannot write to stdout: " + e);
            }
        }
    }
}

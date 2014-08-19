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
        STALENESS_READ("scoutid", "object", "#msgs"),

        STALENESS_WRITE("scoutid", "object"),

        STALENESS_CALIB("rtt_ms", "skew_ms", "scoutid", "client_host", "server_host"),

        STALENESS_YCSB_READ("sessionID", "object", "write_timestamp", "read_timestamp"),

        STALENESS_YCSB_WRITE("sessionID", "object", "write_timestamp"),

        METADATA("session_id", "message_name", "total_message_size", "version_or_update_size", "value_size",
                "batch_independent_global_metadata_size", "batch_dependent_global_metadata_size",
                "batch_size_finest_grained", "batch_size_finer_grained", "batch_size_coarse_grained", "max_vv_size",
                "max_vv_exceptions_num"),

        APP_OP("session_id", "operation_name", "duration_ms"),

        // WISHME: type-safely unify "cause" field across applications?
        APP_OP_FAILURE("session_id", "operation_name", "cause"),

        IDEMPOTENCE_GUARD_SIZE("node_id", "entries"),

        DATABASE_TABLE_SIZE("node_id", "table_name", "size_bytes");

        private final int fieldsNumber;
        private final String semantics;

        private ReportType(String... fieldsSemantics) {
            this.fieldsNumber = fieldsSemantics.length;
            final StringBuilder semanticsBuilder = new StringBuilder();
            for (final String field : fieldsSemantics) {
                if (semanticsBuilder.length() > 0) {
                    semanticsBuilder.append(',');
                }
                semanticsBuilder.append(field);
            }
            this.semantics = semanticsBuilder.toString();
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

    public synchronized static void configure(Properties props) {
        String reportsProp = props.getProperty("swift.reports");
        if (reportsProp == null || reportsProp.isEmpty()) {
            return;
        }
        final EnumSet<ReportType> reports = EnumSet.noneOf(ReportType.class);
        for (final String report : reportsProp.split(",")) {
            try {
                reports.add(ReportType.valueOf(report));
            } catch (IllegalArgumentException x) {
                logger.warning("Unrecognized report type " + report + " - ignoring");
            }
        }
        configure(reports);
    }

    public synchronized static void configure(EnumSet<ReportType> reports) {
        printlnComment("The log includes the following reports:");
        for (final ReportType report : reports) {
            enabledReportsEnumSet.add(report);
            report.printHeader();
            logger.info("Configured report " + report);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SafeLog.close();
            }
        });
    }

    public synchronized static void report(ReportType type, Object... args) {
        if (!type.isEnabled()) {
            return;
        }
        if (args.length != type.getFieldsNumber()) {
            logger.warning("Misformated report " + type + ": " + args);
        }
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

    public synchronized static void flush() {
        try {
            bufferedOutput.flush();
        } catch (IOException e) {
            logger.warning("Cannot flush stdout: " + e);
        }
    }

    public synchronized static void close() {
        try {
            bufferedOutput.close();
            enabledReportsEnumSet.clear();
        } catch (IOException e) {
            logger.warning("Cannot close stdout: " + e);
        }
    }

    public synchronized static void printlnComment(String comment) {
        try {
            bufferedOutput.write(COMMENT_CHAR);
            bufferedOutput.write(comment);
            bufferedOutput.write('\n');
        } catch (IOException e) {
            logger.warning("Cannot write to stdout: " + e);
        }
    }
}

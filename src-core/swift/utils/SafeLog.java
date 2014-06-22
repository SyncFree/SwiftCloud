package swift.utils;

import java.io.PrintStream;

/**
 * Shared logging class for safe reporting of predefined types of values from
 * concurrent threads.
 * 
 * @author mzawirski
 */
public class SafeLog {
    public static final char COMMENT_CHAR = '#';

    // TODO: alternatively, just inline these things as methods for type safety
    public enum ReportType {
        STALENESS_READ("%s,%s,%d", "scoutid,object,#msgs"),

        STALENESS_WRITE("%s,%s", "scoutid,object"),

        STALENESS_CALIB("%d,%d,%s,%s,%s", "rtt_ms,skew_ms,scoutid,client_host,server_host"),

        METADATA("%s,%s,%d,%d,%d,%d,%d,%d,%d", "timestamp_ms,session_id,message_name,"
                + "total_message_size,version_or_update_size,value_size,"
                + "explicitly_computed_global_metadata,batch_size,max_vv_size,max_vv_exceptions_num"),

        APP_OP("%s,%s,%d", "session_id,operation_name,duration_ms");

        private final String format;
        private final String semantics;

        private ReportType(String format, String semantics) {
            this.format = format;
            this.semantics = semantics;
        }

        public String getFormat() {
            return format;
        }

        public void printHeader() {
            bufferedOutput.printf("%c report type %s formatted as timestamp_ms,%s,%s\n", COMMENT_CHAR, name(), name(),
                    semantics);
        }
    }

    private static final PrintStream bufferedOutput = new PrintStream(System.out, false);

    public static void printHeader() {
        for (final ReportType type : ReportType.values()) {
            type.printHeader();
        }
    }

    public static void report(ReportType type, Object... args) {
        bufferedOutput.printf("%d,%s,%s\n", System.currentTimeMillis(), type.name(),
                String.format(type.getFormat(), args));
    }

    public static void flush() {
        bufferedOutput.flush();
    }

    public static void printfComment(String format, Object... args) {
        bufferedOutput.printf(COMMENT_CHAR + format, args);
    }

    public static void printlnComment(String comment) {
        bufferedOutput.println(COMMENT_CHAR + comment);
    }
}

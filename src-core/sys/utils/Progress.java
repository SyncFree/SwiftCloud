package sys.utils;

public class Progress {

    public static String percentage(int curr, int total) {
        String now = String.format("%.1f%%", 100.0 * curr / total);
        String before = msg.get();

        if (before.equals(now))
            return before;
        else {
            msg.set(now);
            return now;
        }
    }

    private static final ThreadLocal<String> msg = new ThreadLocal<String>() {
        @Override
        protected String initialValue() {
            return "";
        }
    };

}

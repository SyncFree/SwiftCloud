package sys.utils;

import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Provides a simple customized Logger class.
 * 
 * @author smd
 * 
 */
public class Log {

    protected Log() {
    }

    public static Logger Log;

    static {
        Log = Logger.getAnonymousLogger();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setFormatter(new Formatter() {
                final int TEXT_WIDTH = 100;
                final String padding = "                                               ";

                public String format(LogRecord r) {
                    StringBuilder res = new StringBuilder().append(r.getLevel()).append(" : ").append(r.getMessage());
                    if (r.getLevel() == Level.FINEST) {
                        while (res.length() < TEXT_WIDTH) {
                            int n = Math.min(TEXT_WIDTH - res.length(), padding.length());
                            res.append(padding.substring(0, n));
                        }
                        res.append(callerClass());
                    }
                    return res.append('\n').toString();
                }
        });
        ch.setLevel(Level.ALL);
        Log.addHandler(ch);
        Log.setLevel(Level.ALL);
    }

    /**
     * 
     * 
     * @return name of the calling class that logged the entry...
     */
    private static String callerClass() {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();

        for (int i = stack.length; --i >= 0;) {
            String cn = stack[i].getClassName();
            if (cn.equals(java.util.logging.Logger.class.getName())) {
                String name = stack[i + 1].getClassName();
                // int j = name.lastIndexOf('.');
                // name = j < 0 ? name : name.substring(j+1);
                return String.format("[%5s : %4d]", name, stack[i + 1].getLineNumber());
            }
        }
        return "[]";
    }
   

}

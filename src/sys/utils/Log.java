package sys.utils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Filter;
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
	public static LogFilter filter;

	public static void setLevel(String packagePrefix, Level level) {
		filter.setLevel(packagePrefix, level);
	}

	static {
		Log = Logger.getAnonymousLogger();
		ConsoleHandler ch = new ConsoleHandler();
		ch.setFormatter(new Formatter() {
			final int TEXT_WIDTH = 100;
			final String padding = "                                               ";

			@Override
			public String format(LogRecord r) {
				StringBuilder res = new StringBuilder().append(r.getLevel()).append(" : ").append(r.getMessage());
				while (res.length() < TEXT_WIDTH) {
					int n = Math.min(TEXT_WIDTH - res.length(), padding.length());
					res.append(padding.substring(0, n));
				}
				res.append(callerClass(r));
				return res.append('\n').toString();
			}
		});
        // Do not hardcode logging levels!!!
        // ch.setLevel(Level.ALL);
        // Log.setLevel(Level.ALL);
		Log.setFilter(filter = new LogFilter());
		Log.addHandler(ch);
	}

	/**
	 * 
	 * 
	 * @return name of the calling class that logged the entry...
	 */
	private static String callerClass(LogRecord r) {
		String name = r.getSourceClassName();
		StackTraceElement[] stack = Thread.currentThread().getStackTrace();

		for (int i = stack.length; --i >= 0;)
			if (name.equals(stack[i].getClassName()))
				return String.format("[%5s : %4d]", name, stack[i].getLineNumber());
		return "[]";
	}

	static class LogFilter implements Filter {

		Map<String, Level> levels = new TreeMap<String, Level>(new Comparator<String>() {
			@Override
			public int compare(String a, String b) {
				int v = b.length() - a.length();
				if (v == 0)
					return a.compareTo(b);
				else
					return v;
			}
		});

		Map<String, Integer> resolved = new HashMap<String, Integer>();

		public void setLevel(String regex, Level level) {
			levels.put(regex, level);
		}

		@Override
		public boolean isLoggable(LogRecord r) {
			String className = r.getSourceClassName();
			Integer level = resolved.get(className);
			if (level == null) {
				level = Level.OFF.intValue();
				for (Map.Entry<String, Level> i : levels.entrySet())
					if (className.startsWith(i.getKey())) {
						level = i.getValue().intValue();
						break;
					}
				resolved.put(className, level);
			}
			return r.getLevel().intValue() >= level;
		}

	}
}

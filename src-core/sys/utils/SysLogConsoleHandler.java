package sys.utils;

import java.util.logging.ConsoleHandler;
import java.util.logging.LogRecord;

/**
 * Provides a simple customized Logger class.
 * 
 * @author smd
 * 
 */
public class SysLogConsoleHandler extends ConsoleHandler {

	public SysLogConsoleHandler() {
		this.setFormatter( new SysLogFormatter() ) ;
	}

	private static class SysLogFormatter extends java.util.logging.Formatter {

		private SysLogFormatter() {
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
					return String.format("[%5s : %4d : %.4f]", name, stack[i].getLineNumber(), sys.Sys.Sys.currentTime());
			return "[]";
		}

		@Override
		public String format(LogRecord r) {
			final int TEXT_WIDTH = 200;
			final String padding = "                                               ";

			StringBuilder res = new StringBuilder().append(r.getLevel()).append(" : ").append(r.getMessage());
			while (res.length() < TEXT_WIDTH) {
				int n = Math.min(TEXT_WIDTH - res.length(), padding.length());
				res.append(padding.substring(0, n));
			}
			res.append(callerClass(r));
			return res.append('\n').toString();
		}
	}

}

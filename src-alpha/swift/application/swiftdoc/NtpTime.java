package swift.application.swiftdoc;

import sys.utils.Threading;

import com.google.code.sntpjc.Client;

public class NtpTime {

	static final int ITERATIONS = 5;

	static long offset_ms = 0;

	static public long timeInMillis() {
		return System.currentTimeMillis() + offset_ms;
	}

	static void init() {
		System.err.printf(sys.Sys.Sys.mainClass + "\nSynchronizing clocks...");
		Client ntpClient;
		double offset = Double.MAX_VALUE;
		for (int i = 0; i < ITERATIONS; i++) {
			try {
				ntpClient = new Client("pt.pool.ntp.org");
				offset = Math.min(offset, ntpClient.getLocalOffset());
				Threading.sleep(500);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		offset_ms = (long) (offset * 1e6);
		System.err.printf("offset :%.3f ms\n", 1000 * offset );
		
	}
}

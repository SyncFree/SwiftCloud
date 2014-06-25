package swift.application.tpcw;

import org.uminho.gsd.benchmarks.benchmark.BenchmarkMain;

import swift.dc.DCSequencerServer;
import swift.dc.DCServer;

public class TPCWBenchmark {

    public static void main(String[] args) throws InterruptedException {
        // Initialize DC
        DCSequencerServer.main(new String[] { "-name", "localhost" });
        DCServer.main(new String[] { "-servers", "localhost" });

        String[] benchArgs = { "-d", "swiftcloud", // select database
                "-id", "pop", // populator id
                "-pop" // populate and exit
        };

        BenchmarkMain.main(benchArgs);

        Thread.sleep(10000);

        System.exit(0);

    }
}

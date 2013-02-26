package sys.ec2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Utility class to select the closest node from a list of candidates, based on
 * static RTT data stored on a file...
 * 
 * @author smd
 * 
 */

public class ClosestDomain {

    public static String closest2Domain(Collection<String> candidates) {
        System.err.println(candidates);
        String res = null;
        double bestRTT = Double.MAX_VALUE;
        for (String i : candidates) {
            double rtt = getDomainRTT(i);
            if (rtt < bestRTT) {
                bestRTT = rtt;
                res = i;
            }
            // return i; // HACK HACK TO USE FIRST DC
        }
        return res != null ? res : "localhost";
    }

    static double getDomainRTT(String host) {
        String domain = host.substring(host.indexOf('.') + 1);
        Double rtt = rtts.get(domain);
        return rtt != null ? rtt : Double.MAX_VALUE / 2;
    }

    static SortedMap<String, Double> rtts = new TreeMap<String, Double>();
    static {
        File f = new File(".ec2-rtts");
        if (f.exists()) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(f));
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("---")) {
                        br.readLine();
                        String data = br.readLine();
                        if (data.startsWith("rtt")) {
                            String[] hostDataBits = line.split(" ");
                            String[] dataBits = data.replace('/', ' ').split(" ");
                            String host = hostDataBits[1];
                            double rtt = Double.valueOf(dataBits[7]);
                            String domain = host.substring(host.indexOf('.') + 1);
                            rtts.put(domain, rtt);
                        }
                    }
                }
                br.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        } else {
            System.err.println("WARNING: EC2 RTT DATA MISSING...");
        }
    }

    public static void main(String[] args) {
        System.out.println("Run baby run...");
    }
}

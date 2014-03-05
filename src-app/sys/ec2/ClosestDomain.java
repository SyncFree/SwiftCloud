package sys.ec2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import sys.utils.Args;

/**
 * Utility class to select the closest node from a list of candidates, based on
 * static RTT data stored on a file...
 * 
 * @author smd
 * 
 */

public class ClosestDomain {

    public static String closest2Domain(List<String> candidates, int site) {
        System.err.println("Choosing server from: " + candidates);
        String res = null;
        double bestRTT = Double.MAX_VALUE / 2;
        for (String i : candidates) {
            double rtt = getDomainRTT(i);
            if (rtt < bestRTT) {
                bestRTT = rtt;
                res = i;
            }
            // return i; // HACK HACK TO USE FIRST DC
        }
        if (res == null && site >= 0)
            res = candidates.get(site % candidates.size());

        return res == null ? "localhost" : res;
    }

    static double getDomainRTT(String host) {
        String domain = host.substring(host.indexOf('.') + 1);
        Double rtt = rtts.get(domain);
        return rtt != null ? rtt : Double.MAX_VALUE;
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

    public static void main(String[] args) throws Exception {
        String host = InetAddress.getLocalHost().getHostName();
        List<String> servers = Args.subList(args, "-servers");
        System.out.println(host + "--->" + closest2Domain(servers, 0));
    }
}

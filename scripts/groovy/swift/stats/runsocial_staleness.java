package swift.stats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class runsocial_staleness {
    String[] files;
    Map<String, String> strs; // used for

    Map<String, List<Write>> msgs; // obj key -> list <write>
    Map<String, NetInfo> netinfo;
    Map<String, List<Read>> reads; // obj key -> list <read>

    long maxSkew;
    long maxLatency;

    int[] staleCount; // staleCount[n] -> number of times a read is stale by n
                      // writes
    int[] staleAmount; // staleAmount[n] -> number of times a read is stale by n
                       // ms

    runsocial_staleness(String[] args) {
        files = args;
    }

    public String strs(String str) {
        String s = strs.get(str);
        if (s == null) {
            strs.put(str, str);
            s = str;
        }
        return s;
    }

    private void readFile(String filename) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        for (;;) {
            String line = reader.readLine();
            if (line == null)
                break;
            String[] elems = line.split(",");
            if (elems.length == 5 && elems[1].equals("STALENESS_WRITE")) {
                // STALENESS_WRITE,time,scoutid,key,curtime - 5
                List<Write> l = msgs.get(elems[3]);
                if (l == null) {
                    l = new ArrayList<Write>();
                    msgs.put(elems[3], l);
                }
                l.add(new Write(Long.parseLong(elems[4]), strs(elems[2])));
            } else if (elems.length == 6 && elems[1].equals("STALENESS_READ")) {
                // STALENESS_READ,time,scoutid,key,curtime,size - 6
                List<Read> l = reads.get(elems[3]);
                if (l == null) {
                    l = new ArrayList<Read>();
                    reads.put(elems[3], l);
                }
                l.add(new Read(Long.parseLong(elems[4]), Integer.parseInt(elems[5]), strs(elems[2])));
            } else if (elems.length == 7 && elems[1].equals("STALENESS_CALIB")) {
                // STALENESS_CALIB,time,rtt,skew,scoutid,ip,server_ip - 7
                NetInfo info = netinfo.get(elems[4]);
                if (info == null) {
                    info = new NetInfo(Long.parseLong(elems[3]), Long.parseLong(elems[2]) / 2);
                    netinfo.put(elems[4], info);
                } else
                    info.update(Long.parseLong(elems[3]), Long.parseLong(elems[2]) / 2);
            }

        }
        reader.close();
    }

    private void orderMsgList() {
        Iterator<List<Write>> it = msgs.values().iterator();
        while (it.hasNext()) {
            List<Write> l = it.next();
            Collections.sort(l);
        }
    }

    private void orderReads() {
        Iterator<List<Read>> it = reads.values().iterator();
        while (it.hasNext()) {
            List<Read> l = it.next();
            Collections.sort(l);
        }
    }

    void resetStalenessData() {
        staleCount = new int[0];
        staleAmount = new int[0];
    }

    void incStaleCount(int n) {
        if (staleCount.length - 1 < n) {
            int[] b = new int[(((n + 1) / 100) + 1) * 100];
            System.arraycopy(staleCount, 0, b, 0, staleCount.length);
            staleCount = b;
        }
        staleCount[n]++;
    }

    void incStaleAmount(int n) {
        if (staleAmount.length - 1 < n) {
            int[] b = new int[(((n + 1) / 100) + 1) * 100];
            System.arraycopy(staleAmount, 0, b, 0, staleAmount.length);
            staleAmount = b;
        }
        staleAmount[n]++;
    }

    void computeOneStaleness(String objkey, List<Read> reads) {
        List<Write> writes = msgs.get(objkey);
        Iterator<Read> it = reads.iterator();
        while (it.hasNext()) {
            Read read = it.next();
            if (writes == null) {
                incStaleCount(0);
                incStaleAmount(0);
                continue;
            }

            int pos = Collections.binarySearch(writes, read);
            if (pos < 0)
                pos = -pos - 2;

            int nWrites = 0;
            for (int i = pos; i >= 0; i--) {
                Write w = writes.get(i);
                if (read.time - maxLatency > w.time) {
                    nWrites = nWrites + i + 1;
                    break;
                }
                if (read.scout.equals(w.scout)) {
                    nWrites++;
                } else {
                    if (read.time > w.time + netinfo.get(w.scout).latency)
                        nWrites++;
                }
            }

            int diffN = nWrites - read.size;
            if (diffN < 0)
                diffN = 0;
            incStaleCount(diffN);

            if (diffN == 0)
                incStaleAmount(0);
            else {
                long staleV = read.time - writes.get(pos - diffN + 1).time;
                incStaleAmount((int) staleV);
            }
        }
    }

    void adjustClockSkew() {
        Iterator<Entry<String, List<Read>>> itr = reads.entrySet().iterator();
        while (itr.hasNext()) {
            Entry<String, List<Read>> entry = itr.next();
            Iterator<Read> it2 = entry.getValue().iterator();
            while (it2.hasNext()) {
                Read r = it2.next();
                if (netinfo.get(r.scout) == null)
                    System.out.println(r.scout);
                r.time = r.time + netinfo.get(r.scout).skew;
            }
        }
        Iterator<Entry<String, List<Write>>> itw = msgs.entrySet().iterator();
        while (itw.hasNext()) {
            Entry<String, List<Write>> entry = itw.next();
            Iterator<Write> it2 = entry.getValue().iterator();
            while (it2.hasNext()) {
                Write r = it2.next();
                r.time = r.time + netinfo.get(r.scout).skew;
            }
        }
    }

    void computeMaxs() {
        maxSkew = 0;
        maxLatency = 0;
        Iterator<NetInfo> it = netinfo.values().iterator();
        while (it.hasNext()) {
            NetInfo info = it.next();
            if (info.skew > maxSkew)
                maxSkew = info.skew;
            if (info.latency > maxLatency)
                maxLatency = info.latency;
        }
    }

    void computeStaleness() {
        resetStalenessData();

        Iterator<Entry<String, List<Read>>> it = reads.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, List<Read>> entry = it.next();
            computeOneStaleness(entry.getKey(), entry.getValue());
        }
    }

    void dumpStaleness() {
        long total = 0;
        int lastCount = 0;
        int lastAmount = 0;
        for (int i = 0; i < staleCount.length; i++) {
            total += staleCount[i];
            if (staleCount[i] > 0)
                lastCount = i;
        }
        for (int i = 0; i < staleAmount.length; i++) {
            if (staleAmount[i] > 0)
                lastAmount = i;
        }

        int acum = 0;
        for (int i = 0; i <= lastCount; i++) {
            acum = acum + staleCount[i];
            System.out.println("+\t" + i + "\t" + staleCount[i] + "\t" + acum + "\t"
                    + ((double) ((long) (((double) staleCount[i] / total) * 10000)) / 100.0) + "\t"
                    + ((double) ((long) (((double) acum / total) * 10000)) / 100.0));
        }
        System.err.println("Average stale count = " + ((double) acum / total));

        acum = 0;
        long acumAmount = 0;
        for (int i = 0; i <= lastAmount; i++) {
            if (staleAmount[i] == 0)
                continue;
            acum = acum + staleAmount[i];
            acumAmount = acumAmount + i * staleAmount[i];
            System.out.println("*\t" + i + "\t" + staleAmount[i] + "\t" + acum + "\t"
                    + ((double) ((long) (((double) staleAmount[i] / total) * 10000)) / 100.0) + "\t"
                    + ((double) ((long) (((double) acum / total) * 10000)) / 100.0));
        }
        System.err.println("Average stale amount = " + ((double) acumAmount / total));
    }

    private void doit() throws Exception {
        strs = new HashMap<String, String>();
        msgs = new HashMap<String, List<Write>>();
        netinfo = new HashMap<String, NetInfo>();
        reads = new HashMap<String, List<Read>>();

        for (int i = 0; i < files.length; i++)
            readFile(files[i]);

        adjustClockSkew();

        orderMsgList();
        orderReads();

        computeMaxs();

        computeStaleness();
        dumpStaleness();
    }

    static void getFiles(File base, Set<String> files, String pattern) {
        if (base.isDirectory())
            for (File i : base.listFiles())
                getFiles(i, files, pattern);
        else {
            if (base.getAbsolutePath().contains(pattern) && !base.getName().startsWith("."))
                files.add(base.getAbsolutePath());
        }
    }

    public static void main(String args[]) {
        try {
            String Base = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-reservations/results/staleness/swiftsocial/";

            File base = new File(Base);
            System.err.println(Arrays.asList(base.list()));
            for (File f : base.listFiles()) {
                System.err.println(f);
                Set<String> fileSet = new HashSet<String>();
                getFiles(f, fileSet, ".log");

                System.err.println(fileSet);
                FileOutputStream fos = new FileOutputStream("/tmp/" + f.getName() + "-staleness.txt");
                System.setOut(new PrintStream(fos));
                new runsocial_staleness(fileSet.toArray(new String[0])).doit();
                fos.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class Write implements Comparable<Write> {
    public Write(long time, String scout) {
        super();
        this.time = time;
        this.scout = scout;
    }

    long time;
    String scout;

    @Override
    public int compareTo(Write o) {
        return time == o.time ? scout.compareTo(o.scout) : time < o.time ? -1 : 1;
    }
}

class Read extends Write {
    public Read(long time, int size, String scout) {
        super(time, scout);
        this.size = size;
    }

    // @Override
    // public int compareTo(Read o) {
    // return time == o.time ? scout.compareTo(o.scout) : time < o.time ? -1 :
    // 1;
    // }

    int size;
}

class NetInfo {
    public NetInfo(long skew, long latency) {
        super();
        this.skew = skew;
        this.latency = latency;
    }

    public void update(long skew, long latency) {
        if (this.latency > latency)
            this.latency = latency;
        if (Math.abs(this.skew) > Math.abs(skew))
            this.skew = skew;
    }

    long skew;
    long latency;
}

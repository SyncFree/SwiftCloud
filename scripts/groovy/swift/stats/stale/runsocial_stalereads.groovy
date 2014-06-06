#!/usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar
package swift.stats

import static java.lang.System.*
import static swift.stats.GnuPlot.*
import umontreal.iro.lecuyer.stat.Tally

Stats<Double> stats = new Stats<Double>();

def processFile = { File f, Tally execTime, Tally g_ratio, Tally t_ratio, Tally dc_t_ratio, Tally dc_g_ratio, Tally rtt, Tally threads, Tally users ->
    if( !f.getName().endsWith(".log"))
        return

    Map<Long, List<String[]>> tids = new TreeMap<Long, List<String[]>>();

    f.eachLine { String line ->
        if (line.contains("threads=")) {
            int i = line.indexOf('=')
            double tt = Integer.valueOf(line.substring(i + 1))
            threads.add(tt)
            return
        }
        if (line.contains("numUsers=")) {
            int i = line.indexOf('=')
            double tt = Integer.valueOf(line.substring(i + 1))
            users.add(tt)
            return
        }

        String[] tokens = line.split(",")
        if (line.startsWith("SYS")) {
            if (tokens.length != 8)
                return
            int exec = Integer.valueOf(tokens[3].trim())
            rtt.add(exec)

            Long key = Long.valueOf(tokens[2].trim())
            List<String[]> gets = tids.get(key)
            if (gets == null)
                tids.put(key, gets = new ArrayList<String[]>());
            gets.add(tokens)
        } else {
            if (tokens.length == 4) {
                int exec = Integer.valueOf(tokens[2])
                execTime.add(exec)
            }
        }
    }

    for (List<String[]> sl : tids.values()) {
        double tid = 0
        boolean missed = false, dc_missed = false
        double RTT = Integer.MAX_VALUE
        for (String[] s : sl) {

            tid = Integer.valueOf(s[2].trim())
            if (tid < 10)
                continue

            int _rtt = Integer.valueOf(s[3].trim())
            RTT = Math.min(RTT, _rtt)

            int mu = Integer.valueOf(s[5].trim())
            if (mu > 0)
                missed = true;
            g_ratio.add(mu > 0 ? 100 : 0)
            int dc_mu = Integer.valueOf(s[7].trim())
            if (dc_mu > 0) {
                dc_missed = true
            }
            g_ratio.add(mu > 0 ? 100 : 0)
            dc_g_ratio.add( dc_mu > 0 ? 100 : 0 )
        }
        t_ratio.add(missed ? 100 : 0)
        dc_t_ratio.add( dc_missed ? 100 : 0)
    }
}


def recurseDir
recurseDir = { File f, Tally execTime, Tally g_ratio, Tally t_ratio, Tally dc_g_ratio, Tally dc_t_ratio, Tally rtt, Tally threads, Tally users, String series ->
    if (f.isDirectory())
        for (File f2 : f.listFiles())
            recurseDir.call(f2, execTime, g_ratio, t_ratio, dc_g_ratio, dc_t_ratio, rtt, threads, users, series)
    else {
        Tally t_ratio2 = new Tally("t_ratio")
        Tally g_ratio2 = new Tally("g_ratio")
        Tally dc_t_ratio2 = new Tally("t_ratio")
        Tally dc_g_ratio2 = new Tally("g_ratio")

        processFile.call(f, execTime, g_ratio2, t_ratio2, dc_g_ratio, dc_t_ratio, rtt, threads, users)
        if (g_ratio2.numberObs() > 2) {
            g_ratio.add(g_ratio2.average())
        }
        if (t_ratio2.numberObs() > 2) {
            t_ratio.add(t_ratio2.average())
        }
        if (dc_g_ratio2.numberObs() > 2) {
            dc_g_ratio.add(dc_g_ratio2.average())
        }
        if (dc_t_ratio2.numberObs() > 2) {
            dc_t_ratio.add(dc_t_ratio2.average())
        }
    }
}

def processRootDir = { String dirFilename, String scenario ->
    System.out.println(dirFilename);

    Tally rtt = new Tally("rtt")
    Tally t_ratio = new Tally("t_ratio")
    Tally g_ratio = new Tally("g_ratio")
    Tally dc_t_ratio = new Tally("dc_t_ratio")
    Tally dc_g_ratio = new Tally("dc_g_ratio")
    Tally threads = new Tally("threads")
    Tally users = new Tally("users")
    Tally execTime = new Tally("updates")
    File dir = new File(dirFilename)
    if (dir.exists()) {
        recurseDir.call(dir, execTime, g_ratio, t_ratio, dc_g_ratio, dc_t_ratio, rtt, threads, users, scenario)

        System.err.println(threads.report() + "/" + users.report() + "/" + t_ratio.report())
        double clients = threads.numberObs() * threads.average()
        double dbSize = users.average()
        stats.series("stalereads_vs_clt", "Transactions").add(clients, t_ratio.average(), t_ratio.standardDeviation())
        stats.series("stalereads_vs_clt", "Individual Reads").add(clients, g_ratio.average(), t_ratio.standardDeviation())

        stats.series("stalereads_vs_dbSize", "Transactions").add(dbSize/1000, t_ratio.average(), t_ratio.standardDeviation())
        stats.series("stalereads_vs_dbSize", "Individual Reads").add(dbSize/1000, g_ratio.average(), g_ratio.standardDeviation())

        stats.series("stalereads_vs_dbSize", "Transactions@DC").add(dbSize/1000, dc_t_ratio.average(), dc_t_ratio.standardDeviation())
        stats.series("stalereads_vs_dbSize", "Individual Reads@DC").add(dbSize/1000, dc_g_ratio.average(), dc_g_ratio.standardDeviation())

        // stats.series("latency", "*").add(execTime.average(), 225 * 10 *
        // 1000 / (1000 * thinkTime.average() + rtt.average()));

    } else
        System.err.println("Wrong/Empty directory...");
}

DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/staleReadsWorkload/";

processRootDir(DIR + "Mar23-1364078382192", "25000");
processRootDir(DIR + "Mar23-1363996812089", "22500");
processRootDir(DIR + "Mar23-1363997189559", "20000");
processRootDir(DIR + "Mar23-1363997568050", "17500");
processRootDir(DIR + "Mar23-1363997946592", "15000");
processRootDir(DIR + "Mar23-1363998322820", "12500");
processRootDir(DIR + "Mar23-1363998694841", "10000");
processRootDir(DIR + "Mar23-1363999062547", "7500");
processRootDir(DIR + "Mar23-1364068330246", "5000");
processRootDir(DIR + "Mar23-1364068700298", "2500");


plots = [:]
int n = 0;
for( Series i : stats.series("stalereads_vs_dbSize") ) {
    data = []
    i.size().times  {
        data += String.format("%.3f\t%.3f\t%.3f", i.xValue(it), i.yValue(it), i.eValue(it))
    }
    plots[i.name()] = data
}


def gnuplot = [
    '#set label 1 "SwiftSocial - scout at client" font "Helvetica,16" at 10,95',
    'set terminal postscript size 10.0, 5.0 monochrome dashed font "Helvetica,24" linewidth 1',
    //               'set terminal aqua dashed',
    'set ylabel "Ops w/ stale reads [ % ]"',
    'set xlabel "Social Network DB Size [ users x 1000]"',
    'set mxtics',
    'set mytics',
    'set xr [0.0:25.0]',
    'set yr [0:5]',
    'set pointinterval 10',
    'set key right top',
    'set clip',
    'set lmargin at screen 0.05',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.05',
    'set tmargin at screen 0.85',
    'set grid xtics ytics lt 30 lt 30',
]
String outputFile = "/tmp/sosp/swiftsocial-stalereads"

GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    int w = (k == 'Transactions' ? 4 : 2)
    String.format('title "%s" with errorlines pointinterval 1 lw %s ps 1', k, w)
})

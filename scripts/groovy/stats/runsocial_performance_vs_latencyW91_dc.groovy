#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib:scripts/groovy/stats/lib::scripts/groovy/stats/lib/ssj.jar

import static Tools.*

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import static java.lang.System.*

import umontreal.iro.lecuyer.stat.Tally;
import static GnuPlot.*

Stats<Integer> stats = new Stats<Integer>();
Set<String> skipOps = new HashSet<String>();

def processFile = { int N, int HT, File f, Tally clock, Tally threads, Tally latency, Tally thinkTime ->

    err.println f

    int n = 0;
    long T0 = -1;
    f.eachLine { String line ->
        if (line.startsWith(";") || line.contains("CACHE")) {
            if (line.contains("swiftsocial.thinkTime=")) {
                double tt = Double.valueOf(line.split("=")[1]);
                thinkTime.add(tt);
            }
            if (line.contains("threads=")) {
                double th = Double.valueOf(line.split("=")[1]);
                threads.add(th);
            }
            return
        }
        String[] tok = line.split(",");
        if (tok.length == 4) {
            int tid = Integer.valueOf(tok[0]);
            String op = tok[1];
            if (tid < 0 || skipOps.contains(op))
                return ;
            int execTime = Integer.valueOf(tok[2]);
            long T = Long.valueOf(tok[3]);
            if (T0 < 0)
                T0 = T;
            T -= T0;

            // if (T > LT * 1000 && T < (HT - 15) * 1000) {
            // latency.add(execTime);
            // clock.add(T);
            // L.add(execTime);
            // }
            if (n++ >= N) {
                latency.add(execTime);
                clock.add(T);
            }
        }
    }
}

def recurseDir
recurseDir = { int lt, int ht, final File f, final Tally clock, final Tally threads, final Tally latency, final Tally thinkTime, String scenario ->
    if (f.isDirectory()) {
        for (File f2 : f.listFiles())
            recurseDir.call(lt, ht, f2, clock, threads, latency, thinkTime, scenario)
    } else {
        if (f.getName().endsWith(".log"))
            processFile.call(lt, ht, f, clock, threads, latency, thinkTime)
    }
}

def processRootDir = { int lt, int ht, String dirFilename, String scenario ->
    err.println dirFilename

    Tally threads = new Tally("Threads")
    Tally latency = new Tally("Latency")
    Tally thinkTime = new Tally("thinkTime")
    Tally clock = new Tally("clock")

    File dir = new File(dirFilename);
    if (dir.exists()) {

        recurseDir.call( lt, ht, dir, clock, threads, latency, thinkTime, scenario);

        err.println threads.report();
        err.println latency.report()
        err.println thinkTime.report()
        err.println clock.report()

        double sessions = threads.average() * threads.numberObs();
        double thl = sessions * 1000 / (thinkTime.average() + latency.average());
        double ths = 1000 * clock.numberObs() / (clock.max() - clock.min());
        double lat = latency.average();

        stats.series("AvgThroughput", scenario + "").add(ths * 60 / 1000, lat);
    } else
        err.println("Wrong/Empty directory...");
}


skipOps.add("TOTAL");
skipOps.add("INIT");
skipOps.add("LOGIN");
skipOps.add("LOGOUT");
skipOps.add("ERROR");


DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/SOSP/ionv/workload91/dc/";

        int l = 10;

        processRootDir.call(l,  120, DIR + "Mar16-1363436064864", "1 DC");
        processRootDir.call(l,  120, DIR + "Mar16-1363436761488", "1 DC");
        processRootDir.call(l,  120, DIR + "Mar16-1363437095426", "1 DC");
        processRootDir.call(l,  120, DIR + "Mar16-1363437440149", "1 DC");
        processRootDir.call(l,  120, DIR + "Mar16-1363437913757", "1 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363438757478", "1 DC");

        processRootDir.call(l,  300, DIR + "Mar16-1363445570221", "2 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363446087795", "2 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363446687338", "2 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363449306773", "2 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363448196932", "2 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363448783058", "2 DC");

        processRootDir.call(l,  300, DIR + "Mar16-1363451494533", "3 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363452015812", "3 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363452486286", "3 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363453013965", "3 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363454684838", "3 DC");

        processRootDir.call(l,  300, DIR + "Mar16-1363453675346", "3 DC");
        processRootDir.call(l,  300, DIR + "Mar16-1363454247634", "3 DC");


plots = [:]

for( Series i : stats.series("AvgThroughput") ) {
    data = []
    i.size().times  {
        data += String.format("%.0f\t%.1f", i.xValue(it), i.yValue(it))
    }
    plots[i.name()] = data
}

String outputFile = "/tmp/sosp/swiftsocial-scout-at-dc-w91-lat-vs-perf-load"

def gnuplot = [
    'set encoding utf8',
    '#set label 1 "SwiftSocial - scout at DC" font "Helvetica,16" at 1,950',
    'set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1',
//                   'set terminal aqua dashed',
    'set ylabel "Latency [ ms ]"',
    'set xlabel "TPM [ x 1000 ]"',
    'set mxtics',
    'set mytics',
    'set xr [0.0:40.0]',
    'set yr [0:1000]',
    'set pointinterval 10',
    'set key right top',
    'set label',
    'set lmargin at screen 0.11',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.05',
    'set tmargin at screen 0.9999',
    'set grid xtics ytics lt 30 lt 30',
]

GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String.format('title "%s" with linespoints pointinterval 1 lw 4 ps 2.5', k)
})


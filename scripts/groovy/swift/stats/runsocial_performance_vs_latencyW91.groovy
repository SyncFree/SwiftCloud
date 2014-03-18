#!/usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar
package swift.stats

import static java.lang.System.*
import static swift.stats.GnuPlot.*
import umontreal.iro.lecuyer.stat.Tally

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


DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/SOSP/ionv/workload91/clt/";

int l = 500, h = 120;
processRootDir.call( l, h, DIR +"Mar14-1363299160038", "3 DC-W91");
processRootDir.call( l, h, DIR +"Mar14-1363298777609", "3 DC-W91");
processRootDir.call( l, h, DIR +"Mar14-1363298370556", "3 DC-W91");
processRootDir.call( l, h, DIR +"Mar14-1363297939202", "3 DC-W91");

processRootDir.call( l, h, DIR +"Mar15-1363382925534", "3 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363381854426", "3 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363382386484", "3 DC-W91");

processRootDir.call( l, h, DIR +"Mar15-1363384331754", "2 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363384713504", "2 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363385649786", "2 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363385097700", "2 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363389850554", "2 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363387766424", "2 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363386934296", "2 DC-W91");

processRootDir.call( l, h, DIR +"Mar16-1363392148881", "1 DC-W91");
processRootDir.call( l, h, DIR +"Mar16-1363395534834", "1 DC-W91");
processRootDir.call( l, h, DIR +"Mar16-1363395104669", "1 DC-W91");
processRootDir.call( l, h, DIR +"Mar16-1363396220965", "1 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363391062998", "1 DC-W91");
processRootDir.call( l, h, DIR +"Mar15-1363390595587", "1 DC-W91");


DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/staleReadsWorkload/clt-W99/3DC/";

l = 475; h = 180;
processRootDir.call(l, h, DIR + "Mar25-1364179113928", "3 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364179423304", "3 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364180006214", "3 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364180325093", "3 DC-W99");

processRootDir.call(l, h, DIR + "Mar25-1364180701837", "3 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364181037355", "3 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364181365500", "3 DC-W99");
// processRootDir(l, h, DIR + "Mar25-1364181684653", "3 DC");
// processRootDir(l, h, DIR + "Mar25-1364182005487", "3 DC");

DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/staleReadsWorkload/clt-W99/2DC/";

processRootDir.call(l, h, DIR + "Mar25-1364186192488", "2 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364186494650", "2 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364186805618", "2 DC-W99");

processRootDir.call(l, h, DIR + "Mar25-1364187114563", "2 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364187425100", "2 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364187728065", "2 DC-W99");

processRootDir.call(l, h, DIR + "Mar25-1364188058788", "2 DC-W99");

// processRootDir(l, h, DIR + "Mar25-1364188366719", "2 DC");
// processRootDir(l, h, DIR + "Mar25-1364188724596", "2 DC");

DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/staleReadsWorkload/clt-W99/1DC/";

processRootDir.call(l, h, DIR + "Mar25-1364189156263", "1 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364189465099", "1 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364189772252", "1 DC-W99");

processRootDir.call(l, h, DIR + "Mar25-1364190089832", "1 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364190402256", "1 DC-W99");
processRootDir.call(l, h, DIR + "Mar25-1364190729914", "1 DC-W99");

processRootDir.call(l, h, DIR + "Mar25-1364191066707", "1 DC-W99");


plots = [:]

for( Series i : stats.series("AvgThroughput") ) {
    data = []
    i.size().times  {
        data += String.format("%.0f\t%.1f", i.xValue(it), i.yValue(it))
    }
    plots[i.name()] = data
}


def gnuplot = [
    '#set label 1 "SwiftSocial - scout at client" font "Helvetica,16" at 10,95',
    'set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1',
    //               'set terminal aqua dashed',
    'set ylabel "Latency [ ms ]"',
    'set xlabel "TPM [ x 1000 ]"',
    'set mxtics',
    'set mytics',
    'set xr [0.0:500.0]',
    'set yr [0:100]',
    'set pointinterval 10',
    'set key right top',
    'set lmargin at screen 0.11',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.05',
    'set tmargin at screen 0.9999',
    'set grid xtics ytics lt 30 lt 30',
]
String outputFile = "/tmp/sosp/swiftsocial-scout-at-client-w91w99-lat-vs-perf"

GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String.format('title "%s" with linespoints pointinterval 1 lw 3 ps 2', k)
})

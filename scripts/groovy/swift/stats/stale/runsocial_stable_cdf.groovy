#!/usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar
package swift.stats

import static swift.stats.GnuPlot.*
import umontreal.iro.lecuyer.stat.Tally

Stats<Integer> stats = new Stats<Integer>();
Set<String> skipOps = new HashSet<String>();

def processFile = { File f, int L, int H, String key1, String key2 ->
    println f

    long T0 = -1;
    f.eachLine { String line ->
        if (line.startsWith(";"))
            return;

        String[] tok = line.split(",");
        if (tok.length == 4) {
            int tid = Integer.valueOf(tok[0]);
            String op = tok[1];

            if (tid < 0 || skipOps.contains(op))
                return;

            int execTime = Integer.valueOf(tok[2]);
            long T = Long.valueOf(tok[3]);
            if (T0 < 0)
                T0 = T;
            T -= T0;
            if (T > L * 1000 && T < H * 1000) {
                stats.histogram(key1, key2, 1).tally(execTime, 1.0);
            }
        }
    }
}

def recurseDir
recurseDir = { File f, int L, int H, String key1, String key2 ->
    if (f.isDirectory()) {
        for (File f2 : f.listFiles())
            recurseDir.call(f2, L, H, key1, key2);
    } else {
        if (f.getName().endsWith(".log"))
            processFile.call(f, L, H, key1, key2);
    }
}

def processRootDir = { int L, int H, String dirFilename, String scenario ->
    System.out.println(dirFilename);

    File dir = new File(dirFilename);
    if (dir.exists()) {

        recurseDir.call(dir, L, H, "data", scenario);
    } else
        System.err.println("Wrong/Empty directory...");
}

skipOps.add("TOTAL");
skipOps.add("INIT");
skipOps.add("LOGIN");
skipOps.add("LOGOUT");
skipOps.add("ERROR");


DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/SOSP/stableCDF/"

processRootDir(120, 300, DIR +"Mar18-1363623972595", "sync")
processRootDir(120, 300, DIR + "Mar18-1363624888705", "async")

// processRootDir(120, 600, dir("Mar18-1363650043673",
// "stable sync+nocache-150");
// processRootDir(120, 600, dir("Mar19-1363655170846",
// "stable sync+cache-150");

processRootDir(120, 1200, DIR + "Mar19-1363732390315", "stable sync, nocache")
processRootDir(120, 1200, DIR + "Mar19-1363731100759", "stable sync")
// processRootDir(120, 1200, dir("Mar19-1363733472433") + "",
// "stable sync+nocache-2");


plots = [:]
for( Histogram i : stats.histograms("data") ) {
    data = []
    int n = i.size()
    Number[] xVal = i.xValues()
    Tally[] yVal = i.yValues()

    double total = 0.0
    yVal.each { total += it.sum() }

    double accum = 0.0
    n.times  {
        accum += yVal[it].sum()
        data << String.format("%.0f\t%.1f", xVal[it].doubleValue(), 100 * accum / total)
    }
    plots[i.name()] = data
}



def gnuplot = [
    '#set encoding utf8',
    '#set label 1 "SwiftSocial - scout at DC" font "Helvetica,16" at 1,950',
    'set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1',
    //                   'set terminal aqua dashed',
    'set xlabel "Latency [ ms ]"',
    'set ylabel "Cumulative Ocurrences [ % ]"',
    'set mxtics',
    'set mytics',
    'set xr [0.0:500.0]',
    'set yr [75:100]',
    'set pointinterval 20',
    'set key right bottom',
    'set grid xtics ytics lt 30 lt 30',
    'set label',
    'set clip points',
    'set lmargin at screen 0.11',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.05',
    'set tmargin at screen 0.9999',
]

String outputFile = "/tmp/sosp/stableCDF"

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}


GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String.format('title "%s" with linespoints pointinterval 30 lw 4 ps 2', k)
}, keySorter )




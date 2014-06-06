#!/usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar
package swift.stats

import static swift.stats.GnuPlot.*
import umontreal.iro.lecuyer.stat.Tally

Stats<Integer> stats = new Stats<Integer>();
Set<String> skipOps = new HashSet<String>();

String selectedNode = "dfn-ple1.x-win.dfn.de"

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
        String path = f.absolutePath;
        if (path.endsWith(".log") && path.contains( selectedNode )) {
            //            processFile.call(f, L, H, key1, key2 + "---" + f.getParent());
            processFile.call(f, L, H, key1, key2 );
        }
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


DIR = "results/swiftsocial/multi_cdf/"

//processRootDir(90, 240, DIR +"Mar24-1364147187228", "sync")
processRootDir(90, 240, DIR +"Mar24-1364147758109", "async")
//processRootDir(90, 240, DIR +"Mar24-1364146115900", "stable sync")
processRootDir(90, 900, DIR +"dc-Mar24-1364148988201", "      DC")
processRootDir(90, 600, DIR +"Mar24-1364144788195", "  stable DC")






plots = [:]
for( Histogram i : stats.histograms("data") ) {
    data = []
    int n = i.size()
    Number[] xVal = i.xValues()
    Tally[] yVal = i.yValues()



    double total = 0.0
    yVal.each { total += it.sum() }

    double accum = 0.0, x0 = xVal[0].doubleValue()
    n.times  {
        double y0 = 100 * accum / total
        accum += yVal[it].sum()
        double x = xVal[it].doubleValue(), y = 100 * accum / total
        data << String.format("%.0f\t%.1f", x, y0)
        data << String.format("%.0f\t%.1f", x, y)
        println x + "  " + y
    }
    plots[i.name()] = data
}

def gnuplot = [
    '#set encoding utf8',
    '#set label 1 "SwiftSocial - scout at DC" font "Helvetica,16" at 1,950',
    'set terminal postscript size 10.0, 7.0 monochrome dashed font "Helvetica,28" linewidth 1',
    //                   'set terminal aqua dashed',
    'set xlabel "Latency [ ms ]"',
    'set ylabel "Cumulative Ocurrences [ % ]"',
    'set mxtics',
    'set mytics',
    'set xr [0.0:200.0]',
    'set yr [0:100]',
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

String outputFile = "/tmp/sosp/latency_singleNodeCDF"

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}


GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String lw = k.toString().contains("---") ? 1: 3
    String.format('title "%s" with lines lw %s', k, lw)
    //    String.format('notitle  with lines lw %s', lw)
}, keySorter )




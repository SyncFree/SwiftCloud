#!/usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar
package swift.stats

import static swift.stats.GnuPlot.*
import umontreal.iro.lecuyer.stat.Tally

Stats<Integer> stats = new Stats<Integer>();
Set<String> skipOps = new HashSet<String>();
Set<String> excludeSet = new HashSet<String>();

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
        if( excludeSet.contains( f.getName() ) )
            return

        for (File f2 : f.listFiles())
            recurseDir.call(f2, L, H, key1, key2);
    } else {
        if (f.getName().endsWith(".log")) {
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

//processRootDir(90, 240, DIR +"Mar24-1364135177453", "stable sync, nocache")
//
//processRootDir(90, 240, DIR +"Mar24-1364135558815", "stable sync")

//excludeSet =["planetlab2.byu.edu", "planetlab1.byu.edu"] as Set

//processRootDir(90, 240, DIR +"Mar24-1364138719730", "async")
//processRootDir(90, 240, DIR +"Mar24-1364139147393", "sync")
//processRootDir(90, 240, DIR +"Mar24-1364140006246", "stable sync")
//processRootDir(90, 240, DIR +"Mar24-1364140597506", "stable sync, no cache")

//processRootDir(90, 240, DIR +"Mar24-1364138719730", "async")
//processRootDir(90, 240, DIR +"Mar24-1364139147393", "sync")
//processRootDir(90, 240, DIR +"Mar24-1364140006246", "stable sync")
//processRootDir(90, 240, DIR +"Mar24-1364141876377", "stable sync, no cache")

//processRootDir(90, 240, DIR +"Mar24-1364147187228", "sync")
processRootDir(90, 240, DIR +"Mar24-1364147758109", "async")
//processRootDir(90, 240, DIR +"Mar24-1364146115900", "stable sync")
processRootDir(90, 900, DIR +"dc-Mar24-1364148988201", "      DC")
processRootDir(90, 600, DIR +"Mar24-1364144788195", "  stable DC")





//processRootDir(120, 300, DIR +"Mar18-1363623972595", "sync")
//processRootDir(120, 300, DIR + "Mar18-1363624888705", "async")
//
//// processRootDir(120, 600, dir("Mar18-1363650043673",
//// "stable sync+nocache-150");
//// processRootDir(120, 600, dir("Mar19-1363655170846",
//// "stable sync+cache-150");
//
//processRootDir(120, 1200, DIR + "Mar19-1363732390315", "stable sync, nocache")
//processRootDir(120, 1200, DIR + "Mar19-1363731100759", "stable sync")
//// processRootDir(120, 1200, dir("Mar19-1363733472433") + "",
//// "stable sync+nocache-2");

double X = Double.NEGATIVE_INFINITY;
String N ;
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
        double x = xVal[it].doubleValue(), y = 100 * accum / total
        data << String.format("%.0f\t%.1f", x, y)
        if( y > 95 && y < 98 && x > X ) {
            X = x;
            N = i.name();
        }
    }
    plots[i.name()] = data
}

println "--->" + N

def gnuplot = [
    '#set encoding utf8',
    'set terminal postscript size 10.0, 7.0 monochrome enhanced dl 3 font "Helvetica,24" linewidth 1.25',
    '#set terminal pdf monochrome size 10.0, 5.0 monochrome dashed font "Helvetica,26" linewidth 2',
    'set xlabel "Latency [ ms ]"',
    'set ylabel "Cumulative Ocurrences [ % ]" offset 2,0',
    'set ytics 10',
    'unset mxtics',
    'unset mytics',
    'set xr [0.0:300.0]',
    'set yr [0:100]',
    'set pointinterval 20',
    'set key right bottom',
    '#set grid ytics lt 30',
    'set label',
    'set clip points',
    'set lmargin at screen 0.11',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.06',
    'set tmargin at screen 0.9999',
]

String outputFile = "/tmp/sosp/latency_rw_allnodesCDF"

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}


GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String.format('title "%s" with linespoints pointinterval 5 lw 2 ps 2', k)
}, keySorter )




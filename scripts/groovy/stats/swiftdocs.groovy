#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib:scripts/groovy/stats/lib::scripts/groovy/stats/lib/ssj.jar

import static Tools.*

import java.io.File;

import umontreal.iro.lecuyer.stat.Tally;
import static GnuPlot.*

Stats<Integer> stats = new Stats<Integer>();
Set<String> wantedOps = new HashSet<String>();

def processFile = { File f, int N, String key ->
    println f

    int n = 0;
    f.eachLine { String line ->
        if (line.startsWith("#"))
            return;

        if (++n > N ) {
            int execTime = Integer.valueOf( line.trim() )
            stats.histogram("data", key, 1).tally(execTime, 1.0);
        }
    }
}

def processDir = { int N, String dirName, String scenario ->
    File dir = new File( dirName )

    if (!dir.exists()) {
        System.err.println("Wrong Results Folder");
        System.exit(0);
    } else
        for (File i : dir.listFiles()) {
            processFile.call(i, N, scenario);
        }
}


DIR = "/Users/smd/Dropbox/git/SwiftCloud-1PC/SwiftCloud-1PC/results/swiftdoc/";

def SOA = "SOA"
def CDN = "OurSystem - CDN"
def CLT = "OurSystem"

processDir.call(20, DIR +"Oct1351619021", SOA);
processDir.call(20, DIR +"Oct1351620049", CLT);

//dc_dirResults = new File(basePath + "Oct1351619021");
//processDir("scouts@dc", dc_dirResults);
//dc_dirResults = new File(basePath + "Oct1351620049");
//processDir("scouts@dc", dc_dirResults);
//dc_dirResults = new File(basePath + "Oct1351620298");
//processDir("scouts@dc", dc_dirResults);
//dc_dirResults = new File(basePath + "Oct1351620536");
//processDir("scouts@dc", dc_dirResults);
//dc_dirResults = new File(basePath + "Oct1351621488");
//processDir("scouts@dc", dc_dirResults);
//
//dc_dirResults = new File(basePath + "Oct1351621960");
//processDir("scouts@dc", dc_dirResults);
//dc_dirResults = new File(basePath + "Oct1351622217");
//processDir("scouts@dc", dc_dirResults);
//dc_dirResults = new File(basePath + "Oct1351622498");
//processDir("scouts@dc", dc_dirResults);
//dc_dirResults = new File(basePath + "Oct1351622217");
//processDir("scouts@dc", dc_dirResults);
//dc_dirResults = new File(basePath + "Oct1351622782");
//processDir("scouts@dc", dc_dirResults);


processDir.call(20, DIR +"Oct1351623306", CDN);
processDir.call(20, DIR +"Oct1351623775", CDN);
processDir.call(20, DIR +"Oct1351624042", CDN);
processDir.call(20, DIR +"Oct1351624328", CDN);
processDir.call(20, DIR +"Oct1351624571", CDN);
processDir.call(20, DIR +"Oct1351624839", CDN);
processDir.call(20, DIR +"Oct1351625090", CDN);
processDir.call(20, DIR +"Oct1351625327", CDN);
processDir.call(20, DIR +"Oct1351627572", CDN);
processDir.call(20, DIR +"Oct1351625903", CDN);


def pstyles = [:]

pstyles[SOA]='with linespoints pointinterval 3 lw 4 ps 3 pt 6'
pstyles[CDN]='with linespoints pointinterval 4 lw 4 ps 3 pt 9'
pstyles[CLT]='with linespoints pointinterval 2 lw 4 ps 3 pt 2'

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
    }
    plots[i.name()] = data
}

def gnuplot = [
    '#set encoding utf8',
    'set terminal postscript size 10.0, 5.0 monochrome dl 1 font "Helvetica,24" linewidth 1.25',
    '#set terminal pdf monochrome size 10.0, 5.0 monochrome dashed font "Helvetica,26" linewidth 2',
    'set xlabel "Latency [ ms ]"',
    'set ylabel "Cumulative Ocurrences [ % ]" offset 2,0',
    'set ytics 10',
    'unset mxtics',
    'unset mytics',
    'set xr [0.0:120.0]',
    'set yr [0:100]',
    'set pointinterval 20',
    'set key left bottom',
    '#set grid ytics lt 30',
    'set label',
    'set clip points',
    'set lmargin at screen 0.11',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.06',
    'set tmargin at screen 0.9999',
]

String outputFile = "/tmp/sosp/swiftdocs-cdn-cdf"

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}


GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String sty = pstyles[k]
    String.format('title "%s" %s', k, sty)
}, keySorter )




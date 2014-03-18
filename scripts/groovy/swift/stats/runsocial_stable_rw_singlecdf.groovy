#!/usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar
package swift.stats

import static swift.stats.GnuPlot.*
import umontreal.iro.lecuyer.stat.Tally

Stats<Integer> stats = new Stats<Integer>();
Set<String> wantedOps = new HashSet<String>();

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

            if (tid < 0 || !wantedOps.contains(op))
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

DIR = "results/swiftsocial/multi_cdf/"

def rOS = "  reads, OurSystem"
def wOS = "  writes, OurSystem"

def rOSsoaFT = "reads, OurSystem soaFT"
def wOSsoaFT = "writes, OurSystem soaFT"

def rSoaNoFT = "reads, SOA noFT"
def wSoaNoFT = "writes, SOA noFT"

def rSoaFT = "reads, SOA FT"
def wSoaFT = "writes, SOA FT"

//processRootDir(90, 240, DIR +"Mar24-1364147187228", "sync")

wantedOps = ['READ', 'SEE_FRIENDS'] as Set
processRootDir(90, 240, DIR +"Mar24-1364147758109", rOS)

wantedOps = [
    'POST',
    'STATUS',
    'BEFRIEND'] as Set
processRootDir(90, 240, DIR +"Mar24-1364147758109", wOS)
//
////processRootDir(90, 240, DIR +"Mar24-1364147758109", "async")
wantedOps = ['READ', 'SEE_FRIENDS'] as Set
processRootDir(90, 240, DIR +"Mar24-1364146115900", rOSsoaFT)

wantedOps = [
    'POST',
    'STATUS',
    'BEFRIEND'] as Set
processRootDir(60, 210, DIR +"Mar24-1364146115900", wOSsoaFT)


wantedOps = ['READ', 'SEE_FRIENDS'] as Set
processRootDir(90, 900, DIR +"dc-Mar24-1364148988201", rSoaNoFT)

wantedOps = [
    'POST',
    'STATUS',
    'BEFRIEND'] as Set
processRootDir(90, 900, DIR +"dc-Mar24-1364148988201", wSoaNoFT)


wantedOps = ['READ', 'SEE_FRIENDS'] as Set
processRootDir(90, 600, DIR +"Mar24-1364144788195", rSoaFT)

wantedOps = [
    'POST',
    'STATUS',
    'BEFRIEND'] as Set
processRootDir(90, 600, DIR +"Mar24-1364144788195", wSoaFT)


def pstyles = [:]

pstyles[rOS]='with linespoints pointinterval 11 lw 3 ps 2 pt 7'
pstyles[wOS]='with linespoints pointinterval 7 lw 3 ps 2 pt 6'

pstyles[rOSsoaFT]='with linespoints pointinterval 4 lw 3 ps 2 pt 9'
pstyles[wOSsoaFT]='with linespoints pointinterval 9 lw 3 ps 2 pt 8'

pstyles[rSoaNoFT]='with linespoints pointinterval 5 lw 3 ps 2 pt 13'
pstyles[wSoaNoFT]='with linespoints pointinterval 2 lw 3 ps 2 pt 12'

pstyles[rSoaFT]='with linespoints pointinterval 8 lw 3 ps 2 pt 5'
pstyles[wSoaFT]='with linespoints pointinterval 19 lw 3 ps 2 pt 4'




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
        double x = xVal[it].doubleValue(), y = (int)(100 * accum / total)
        data << String.format("%.0f\t%.1f", x, y0)
        data << String.format("%.0f\t%.1f", x, y)
        println x + "  " + y
    }
    plots[i.name()] = data
}

def gnuplot = [
    '#set encoding utf8',
    'set terminal postscript size 10.6, 7.0 monochrome enhanced font "Helvetica,24" linewidth 1.25',
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

String outputFile = "/tmp/sosp/latency_rw_singleNodeCDF"

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}


GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String sty = pstyles[k]
    String.format('title "%s" %s', k, sty)
}, keySorter )



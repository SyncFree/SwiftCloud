#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib:scripts/groovy/stats/lib::scripts/groovy/stats/lib/ssj.jar

import static Tools.*

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import static java.lang.System.*

import umontreal.iro.lecuyer.stat.Tally;
import static GnuPlot.*

import static System.err

Stats<Integer> stats = new Stats<Integer>();
Set<String> wantedOps = new HashSet<String>();


def processFile = { int L, int H, File f, String placement, String series ->
    err.println f

    long T0 = -1;
    Tally ll = new Tally("latency");
    f.eachLine { String line ->
        if (line.startsWith(";") ) {
            err.println line
            return
        }
        String[] tok = line.split(",")
        if (tok.length == 4) {
            String op = tok[1]
            if (!wantedOps.contains(op))
                return

            int execTime = Integer.valueOf(tok[2])
            long T = Long.valueOf(tok[3])
            T0 = T0 < 0 ? T : T0
            T -= T0
            if (T >= L * 1000 && T <= H * 1000) {
                ll.add(execTime)
                stats.histogram(placement, series, 1).tally(execTime, 1.0)
            }
        }
    }
    err.println(ll.report());
}

def processDir
processDir = { int L, int H, File f, String selector, String topology, String series ->
    if (f.exists()) {
        if (f.isDirectory()) {
            for (File i : f.listFiles()) {
                processDir.call(L, H, i, topology, selector, series);
            }
        } else {
            String name = f.getAbsolutePath();
            if (name.endsWith(".log") && name.contains(selector))
                processFile.call(L, H, f, topology, series)
        }
    } else
        err.println("Wrong directory...: " + f);
}

def String key

key = 'true-' + CACHEPOLICY + '-' + ISOLATION
wantedOps = ['READ', 'SEE_FRIENDS'] as Set
processDir(30, 1000, DIR, key, "*", "reads + async")

wantedOps = [ 'POST', 'STATUS', 'BEFRIEND'] as Set
processDir.call(30, 1000, DIR, key, "*", "updates + async")

key = 'false-' + CACHEPOLICY + '-' + ISOLATION
wantedOps = ['READ', 'SEE_FRIENDS'] as Set
processDir.call(30, 1000, DIR, key, "*", "reads + sync")

wantedOps = [ 'POST', 'STATUS', 'BEFRIEND'] as Set
processDir.call(30, 1000, DIR, key, "*", "updates + sync")

key = 'dc-'
wantedOps = [ 'POST', 'STATUS', 'BEFRIEND', 'READ', 'SEE_FRIENDS'] as Set
processDir.call(30, 1000, DC, key, "*", "scout in DC")


plots = [:]
for( Histogram i : stats.histograms("*") ) {
    data = []
    int n = i.size()
    Number[] xVal = i.xValues()
    Tally[] yVal = i.yValues()

    double total = 0.0
    yVal.each {
        total += it.sum()
    }

    double accum = 0.0
    n.times  {
        accum += yVal[it].sum()
        data << String.format("%.0f\t%.1f", xVal[it].doubleValue(), 100 * accum / total)
    }
    plots[i.name()] = data
}

def gnuplot = [
    'set encoding iso_8859_1',
    'set terminal postscript size 10.0, 6.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1',
    //                   'set terminal aqua dashed',
    'set xlabel "Latency [ ms ]"',
    'set ylabel "Cumulative Ocurrences [ % ]"',
    'set mxtics',
    'set mytics',
    'set xr [0.0:150.0]',
    'set yr [75:100]',
    'set pointinterval 20',
    'set key right bottom',
    'set grid xtics ytics lt 30 lt 30',
    'set label',
    'set clip points',
    'set lmargin at screen 0.1',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.0',
    'set tmargin at screen 0.9999',
]

String outputFile = '/tmp/sosp/SwiftSocial-' + ISOLATION
outputFile = outputFile.replace('_', '-')
//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}


GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String.format('title "%s" with linespoints pointinterval 4 lw 3 ps 2', k)
}, keySorter )


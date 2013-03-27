#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib:scripts/groovy/stats/lib::scripts/groovy/stats/lib/ssj.jar

import static Tools.*
import static GnuPlot.*

File data = new File("results/swiftsocial/SOSP/FailOver/Mar20-1363777521845/pl4.cs.unm.edu/1pc-results-swiftsocial-DC-2-SC-1-TH-1.log")
File dst = File.createTempFile("failover", "data");


series = []

long T0 = -1
data.eachLine { String l ->
    if( ! l.startsWith(";") && !l.startsWith("SYS") ) {
        String[] tok = l.split(",")
        int lat = Integer.valueOf( tok[2] );
        long T = Long.valueOf(tok[3] ) ;
        if( T0 < 0 )
            T0 = T

        series << String.format("%.3f %d", (T - T0)/1000.0, lat)
    }
};

plots = ['latency': series ]

//   def gnuplot = [
//       'set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1',
//       'set ylabel "Latency [ ms ]"',
//       'set xlabel "Time [ s ]"',
//       'set mxtics',
//       'set mytics',
//       'set grid xtics ytics lt 30 lt 30',
//       'plot "' + dst.absolutePath + '" notitle',
//       ]

def gnuplot = [
    'set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1',
    'set ylabel "Latency [ ms ]"',
    'set xlabel "Time [ s ]"',
    'set mxtics',
    'set mytics',
    'set grid xtics ytics lt 30 lt 30',
    'set xr [0.0:80.0]',
    'set yr [0:400]',
    'set lmargin at screen 0.11',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.05',
    'set tmargin at screen 0.9999',
    'set grid xtics ytics lt 30 lt 30',
]
String outputFile = "/tmp/sosp/handoff-plot"

GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String.format('notitle with points ps 1')
})

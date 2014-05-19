#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar "$0" $@; exit $?
package swift.stats

import static swift.stats.GnuPlot.*

if (args.length < 1) {
    println "usage: script <rootdir_with_logs> [more root dirs ...]"
    System.exit(1)
}

DIRs = args.toList()

dataMap = [:]

def recurseDir
recurseDir = { File f->
    if (f.isDirectory()) {
        for (File subF: f.listFiles())
            recurseDir.call(subF);
    } else {
        String path = f.absolutePath;
        if (path.endsWith(".log")) {
            MetadataLogsProcessor.processFile(f, dataMap, null);
        }
    }
}

def processRootDir = { String dirFilename ->
    println "Processing files in directory " + dirFilename

    File dir = new File(dirFilename);
    if (dir.exists()) {
        recurseDir.call(dir);
    } else {
        System.err.println("Wrong/Empty directory...");
        System.exit(1)
    }
}

DIRs.each { dir->
    processRootDir(dir)

    def gnuplot = [
        'set terminal postscript size 10.0, 4.0 enhanced dashed font "Helvetica,24" linewidth 1',
        'set ylabel "Size [ b ]"',
        'set xlabel "Time [ s ]"',
        'set mxtics',
        'set mytics',
        //    'set clip',
        //    'set grid xtics ytics lt 30 lt 30',
        // 'set xr [0.0:80.0]',
        // 'set yr [0:400]',
        'set lmargin at screen 0.11',
        'set rmargin at screen 0.99',
        'set bmargin at screen 0.05',
        'set tmargin at screen 0.9999',
        'set grid xtics ytics lt 30 lt 30',
    ]

    dataMap.each { category, data ->
        data.each { messageType, sessionsSeries ->
            String outputFile = new File(dir, "metadata-" + category.replace(' ', '_')  + "-" + messageType).absolutePath

            GnuPlot.doGraph( outputFile, gnuplot, sessionsSeries, { k, v ->
                String.format('notitle with points ps 1')
            })
            println "Wrote " + outputFile
        }
    }
}

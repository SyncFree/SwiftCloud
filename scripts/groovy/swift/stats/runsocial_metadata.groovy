#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar "$0" $@; exit $?
package swift.stats

import static swift.stats.GnuPlot.*

if (args.length < 1) {
    println "usage: script <rootdir_with_logs>"
    System.exit(1)
}

configsCategoriesMessagesTallyMap = [:].withDefault { config-> [:] }

def decodeConfig = { File dir ->
    //    int DCNodes = 0
    //    dir.name.find(~/DC-(\d+)/) { match, value ->
    //    DCNodes = Integer.valueOf(value)
    //    }
    int clientNodes = 0
    int threadsPerNode = 0
    dir.name.find(~/SC-(\d+)/, { match, value ->
        clientNodes = Integer.valueOf(value)
    })
    dir.name.find(~/TH-(\d+)/, { match, value ->
        threadsPerNode = Integer.valueOf(value)
    })
    clients = threadsPerNode * clientNodes
    if (clients == 0) {
        System.err.println("Error: Cannot decode configuration from directory name " + dir.name)
        System.exit(1)
    }
    return clients.toString()
}

def processExpDir
processExpDir = { File f, String config ->
    if (f.isDirectory()) {
        for (File subF: f.listFiles())
            processExpDir.call(subF, config);
    } else {
        String path = f.absolutePath;
        if (path.endsWith(".log")) {
            MetadataLogsProcessor.processFile(f, null, configsCategoriesMessagesTallyMap[config]);
        }
    }
}

def processAllDirs = { List dirnames ->
    println "Processing files in directories " + dirnames

    dirnames.each { dirname ->
        File dir = new File(dirname);
        if (dir.exists()) {
            processExpDir(dir, decodeConfig(dir))
        } else {
            System.err.println("Error: wrong/empty directory " + dirname);
            System.exit(1)
        }
    }
}

processAllDirs(args.toList())

// Translate into gnuplot series
categoriesMessagesPlotsMap = [:].withDefault { k -> [:].withDefault { []}}
configsCategoriesMessagesTallyMap.each{ config, categoriesMessagesTally ->
    categoriesMessagesTally.each { category, messages ->
        messages.each { message, tally ->
            avg = tally.average()
            // TODO: add stddev etc.
            categoriesMessagesPlotsMap[category][message] << String.format('"%s" %.3f', config, avg)
        }
    }
}


// TODO adjust plot type
def gnuplot = [
    'set terminal postscript size 10.0, 4.0 enhanced dashed font "Helvetica,24" linewidth 1',
    'set ylabel "Metadata size [ b ]"',
    'set xlabel "#clients "',
    'set boxwidth 0.95 relative',

    //    'set clip',
    //    'set grid xtics ytics lt 30 lt 30',
    // 'set xr [0.0:80.0]',
    // 'set yr [0:400]',
    //    'set lmargin at screen 0.11',
    //    'set rmargin at screen 0.99',
    //    'set bmargin at screen 0.05',
    //    'set tmargin at screen 0.9999',
    //    'set grid xtics ytics lt 30 lt 30',
]

categoriesMessagesPlotsMap.each { category,messagesPlots ->
    messagesPlots.each { message, plot ->
        String outputFile = new File(args[0], "metadata-processed-" + message + "-" + category.replace(' ', '_')).absolutePath

        GnuPlot.doGraph( outputFile, gnuplot, ['x':plot], { k, v ->
            String.format('u 1:2 with boxes lc rgb"green" notitle')
        })
        println "Wrote " + outputFile
    }
}


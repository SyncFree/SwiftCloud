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
            stddev = tally.standardDeviation()
            // TODO: add stddev etc.
            categoriesMessagesPlotsMap[category][message] << String.format('%s %.3f %.3f', config, avg, stddev)
        }
    }
}


// TODO adjust plot type
def gnuplot = [
    'set terminal postscript size 10.0, 4.0 enhanced dashed font "Helvetica,24" linewidth 1',
    'set ylabel "Metadata size [ b ]"',
    'set xlabel "#clients "',
    'set style data histograms',
    'set style histogram cluster',
    'set boxwidth 0.95 relative',
    'set style fill solid 1.0 border -1',

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

def WANTED_MESSAGES = [
    "FetchObjectVersionReply",
    "FetchObjectRequest",
    "BatchUpdatesNotification",
    "BatchCommitUpdatesRequest"] as Set
def WANTED_CATEGORIES = [
    MetadataLogsProcessor.CATEGORY_GLOBAL_METADATA_PRECISE,
    MetadataLogsProcessor.CATEGORY_VECTOR_SIZE,
    MetadataLogsProcessor.CATEGORY_BATCH_SIZE] as Set

categoriesMessagesPlotsMap.each { category,messagesPlots ->
    if (!WANTED_CATEGORIES.contains(category)) {
        return
    }
    messagesPlots.each { message, plot ->
        if (!WANTED_MESSAGES.contains(message)) {
            return
        }
        String outputFile = new File(args[0], "metadata-processed-" + message + "-" + category.replace(' ', '_')).absolutePath
        println plot
        GnuPlot.doGraph( outputFile, gnuplot, ['x':plot], { k, v ->
            String.format('using 1:2 with boxes notitle')
        })
        println "Wrote " + outputFile
    }
}


#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar "$0" $@; exit $?
package swift.stats

import static swift.stats.GnuPlot.*

if (args.length < 1) {
    println "usage: script <rootdir_with_logs>"
    System.exit(1)
}

DIR = args[0]

messagesSessionsSeriesFullSize = [:]
messagesSessionsSeriesGlobalMetadata = [:]
messagesSessionsSeriesObjectMetadata = [:]
messagesSessionsSeriesHolesNum = [:]

def addIntoSeries= { Map messagesSessionsSeries, String message, String sessionId, String entry ->
    def sessionsSeries = messagesSessionsSeries[message]
    if (sessionsSeries == null) {
        sessionsSeries = [:]
        messagesSessionsSeries[message] = sessionsSeries
    }
    def series = sessionsSeries[sessionId]
    if (series == null) {
        series = []
        sessionsSeries[sessionId] = series
    }
    series << entry
}

def processFile = { File f->
    println "Processing file " + f

    long T0 = -1
    f.eachLine { String l ->
        if( ! l.startsWith(";") && !l.startsWith("SYS") && l.contains("METADATA_") ) {
            String[] fields = l.split(",")
            String sessionId = fields[0]
            long T = Long.valueOf(fields[1])
            String message = fields[2].substring("METADATA_".size())
            int messageSize = Integer.valueOf(fields[3])
            int objectMetadataData = Integer.valueOf(fields[4])
            int dataOnly = Integer.valueOf(fields[5])
            int vvHolesNumber = Integer.valueOf(fields[6])
            if (T0 < 0) {
                T0 = T
            }
            addIntoSeries(messagesSessionsSeriesFullSize, message, sessionId, String.format("%.3f %d", (T - T0)/1000.0, messageSize))
            addIntoSeries(messagesSessionsSeriesGlobalMetadata, message, sessionId, String.format("%.3f %d", (T - T0)/1000.0, messageSize - objectMetadataData))
            addIntoSeries(messagesSessionsSeriesObjectMetadata, message, sessionId, String.format("%.3f %d", (T - T0)/1000.0, objectMetadataData- dataOnly))
            addIntoSeries(messagesSessionsSeriesHolesNum, message, sessionId, String.format("%.3f %d", (T - T0)/1000.0, vvHolesNumber))
        }
    }
}

def recurseDir
recurseDir = { File f->
    if (f.isDirectory()) {
        for (File subF: f.listFiles())
            recurseDir.call(subF);
    } else {
        String path = f.absolutePath;
        if (path.endsWith(".log")) {
            processFile.call(f);
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

processRootDir(DIR)

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

def categoriesToData = ["full message size":messagesSessionsSeriesFullSize,
    "global metadata" : messagesSessionsSeriesGlobalMetadata,
    "object metadata":messagesSessionsSeriesObjectMetadata,
    "holes number":messagesSessionsSeriesHolesNum]

categoriesToData.each { category, data ->
    data.each { messageType, sessionsSeries ->
        String outputFile = new File(DIR, "metadata-" + category.replace(' ', '_')  + "-" + messageType).absolutePath

        GnuPlot.doGraph( outputFile, gnuplot, sessionsSeries, { k, v ->
            String.format('notitle with points ps 1')
        })
        println "Wrote " + outputFile
    }
}


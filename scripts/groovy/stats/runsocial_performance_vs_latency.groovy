#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib:scripts/groovy/stats/lib::scripts/groovy/stats/lib/ssj.jar

import static Tools.*
import umontreal.iro.lecuyer.stat.Tally;

Stats<Integer> stats = new Stats<Integer>();
Set<String> skipOps = new HashSet<String>();

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
        if (f.getName().endsWith(".log"))
            processFile.call(f, L, H, key1, key2);
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


DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/SOSP/stableCDF/"

processRootDir(120, 300, DIR +"Mar18-1363623972595", "sync")
processRootDir(120, 300, DIR + "Mar18-1363624888705", "async")

// processRootDir(120, 600, dir("Mar18-1363650043673",
// "stable sync+nocache-150");
// processRootDir(120, 600, dir("Mar19-1363655170846",
// "stable sync+cache-150");

processRootDir(120, 1200, DIR + "Mar19-1363732390315", "stable sync, nocache")
processRootDir(120, 1200, DIR + "Mar19-1363731100759", "stable sync")
// processRootDir(120, 1200, dir("Mar19-1363733472433") + "",
// "stable sync+nocache-2");


plots = [:]

for( Histogram i : stats.histograms("data") ) {
    File of = File.createTempFile("swiftsocial", ".dat")
    pw = of.newPrintWriter()
    int n = i.size()
    Number[] xVal = i.xValues()
    Tally[] yVal = i.yValues()

    double total = 0.0
    yVal.each { total += it.sum() }
    
    double accum = 0.0
    n.times  {
        accum += yVal[it].sum()
        pw.printf("%.0f\t%.1f\n", xVal[it].doubleValue(), 100 * accum / total)
    }
    println of 
    pw.close()
    plots[i.name()] = of
}


def gnuplot = [
    'set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1',
//           'set terminal aqua dashed',
    'set output "/tmp/stableCDF.ps"',
    'set xlabel "Latency [ ms ]"',
    'set ylabel "Cumulative Ocurrences [ % ]"',
    'set mxtics',
    'set mytics',
    'set xr [0.0:500.0]',
    'set yr [75:100]',
    'set pointinterval 10',
    'set key right bottom',
    'set grid xtics ytics lt 30 lt 30',
]

plotline = "plot "
plots.each { String k, File v -> 
    plotline += '"' + v.absolutePath + '" title "' + k + '" with linespoints pointinterval 10,'
}

gnuplot += plotline[0..-2]

File gnuplotScript = File.createTempFile("failover", ".gnuplot")
pw = gnuplotScript.newPrintWriter()
gnuplot.each {
    pw.printf("%s;\n", it)
}
pw.close()
println gnuplotScript.absolutePath

exec([
    "/bin/bash",
    "-c",
    "gnuplot " + gnuplotScript.absolutePath
]).waitFor()

exec([
    "/bin/bash",
    "-c",
    "open /tmp/stableCDF.ps"
])


#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib:scripts/groovy/stats/lib::scripts/groovy/stats/lib/ssj.jar

import static Tools.*
import umontreal.iro.lecuyer.stat.Tally;
import static GnuPlot.*

Stats<Integer> stats = new Stats<Integer>();
Set<String> excludeSet = new HashSet<String>();

def Set<String> wantedOps
def Set<Integer> wantedRandomOps = [1, 3, 5, 7] as Set

def processFile = { File f, int L, int H, String key1, String key2 ->
    println f

    int cache, randomOps;
    boolean readFile = true
    ;
    long T0 = -1;
    f.eachLine { String line ->
        if (line.startsWith(";") || line.contains("CACHE")) {
            if (line.contains("swiftsocial.randomOps=")) {
                randomOps = Integer.valueOf(line.split("=")[1]);
                println( randomOps + "   " + wantedRandomOps.contains( randomOps) + "  " + wantedRandomOps )
                readFile &= wantedRandomOps.contains( randomOps)
            }
            if (line.contains("swift.CacheSize=")) {
                cache = Integer.valueOf(line.split("=")[1]);
                readFile &= cache == 512
            }
            return
        }
        
        if( ! readFile )
            return
        
        String[] tok = line.split(",");
        if (tok.length == 4) {
            int tid = Integer.valueOf(tok[0]);
            String op = tok[1];

            if (tid < 0 || ! wantedOps.contains(op))
                return;

            int execTime = Integer.valueOf(tok[2]);
            long T = Long.valueOf(tok[3]);
            if (T0 < 0)
                T0 = T;
            T -= T0;
            if (T > L * 1000 && T < H * 1000) {
                stats.histogram("" +cache, key2 + ", " + randomOps*10 + "%", 1).tally(execTime, 1.0);
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


DIR = "/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/workload-cache/"

def rOS = "  reads, OurSystem"
def wOS = "  writes, OurSystem"

def rOSsoaFT = "reads, OurSystem soaFT"
def wOSsoaFT = "writes, OurSystem soaFT"

def rSoaNoFT = "reads, SOA noFT"
def wSoaNoFT = "writes, SOA noFT"

def rSoaFT = "reads, SOA FT"
def wSoaFT = "writes, SOA FT"

wantedOps = [
    'READ',
    'SEE_FRIENDS',
    'POST',] as Set

processRootDir(90, 240, DIR , "reads")

wantedOps = [
    'POST',
    'STATUS',
    'BEFRIEND'] as Set
processRootDir(90, 240, DIR , "writes")


//def pstyles = [rOS:'7', wOS:6, rOSsoaFT:9, wOSsoaFT:8, rSoaNoFT:13, wSoaNoFT:12, rSoaFT:5, wSoaFT:4]

def pstyles = [:]

pstyles[rOS]='with linespoints pointinterval 26 lw 3 ps 2.5 pt 7'
pstyles[wOS]='with linespoints pointinterval 17 lw 3 ps 2.5 pt 6'

pstyles[rOSsoaFT]='with linespoints pointinterval 17 lw 3 ps 2.5 pt 9'
pstyles[wOSsoaFT]='with linespoints pointinterval 29 lw 3 ps 2.5 pt 8'

pstyles[rSoaNoFT]='with linespoints pointinterval 30 lw 3 ps 2.5 pt 13'
pstyles[wSoaNoFT]='with linespoints pointinterval 25 lw 3 ps 2.5 pt 12'

pstyles[rSoaFT]='with linespoints pointinterval 23 lw 3 ps 2.5 pt 5'
pstyles[wSoaFT]='with linespoints pointinterval 29 lw 3 ps 2.5 pt 4'

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
for( Histogram i : stats.histograms("512") ) {
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
    'set terminal postscript size 10.0, 6.0 monochrome enhanced dl 3 font "Helvetica,24" linewidth 1.25',
    '#set terminal pdf monochrome size 10.0, 5.0 monochrome dashed font "Helvetica,26" linewidth 2',
    'set xlabel "Latency [ ms ]"',
    'set ylabel "Cumulative Ocurrences [ % ]" offset 2,0',
    'set ytics 10',
    'unset mxtics',
    'unset mytics',
    'set xr [0.0:150.0]',
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

String outputFile = "/tmp/sosp/swiftsocial-workload-hitratio"

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}


GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String sty = pstyles[k] ?: "with linespoints pointinterval 10 lw 3"
    String.format('title "%s" %s', k, sty)
}, keySorter )




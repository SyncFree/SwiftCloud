#!/bin/bash
name=social-responsiveness-config-RR-result-social
data=$name-stats.txt

grun=${name}-commands.gp
epsrun=${name}-commands.eps

#set boxwidth 0.75 absolute

echo "set term postscript eps enhanced color 22" > ${grun}
echo "set output \"${epsrun}\"" >> ${grun}
echo "set size 1,1.1" >> ${grun}
echo "set xlabel \"Setting\"" >> ${grun}
echo "set ylabel \"Time [ms]\"" >> ${grun}
echo "set pointsize 3" >> ${grun}
echo "set key invert reverse Left outside" >>{grun}

echo "set style data histograms" >> ${grun}
echo "unset xtics"  >> ${grun}
echo "set xtics nomirror rotate by -45 scale 0 font \",18\""  >> ${grun}
echo "set key outside right top vertical Left reverse noenhanced autotitles columnhead nobox" >> ${grun}


echo "set yrange [0:]" >> ${grun}
echo "set style histogram rowstacked title offset character 0, 0, 0" >> ${grun} 
echo "set style fill solid border -1" >> ${grun}
echo "set boxwidth 0.75 relative" >> ${grun}

echo "i=5" >> ${grun}
echo -n "plot \"${data}\" using 9 fs pattern 1, '' using 6 fs pattern 2,'' using 3 fs pattern 5, '' using 7 fs pattern 4,'' using 8:xtic(10)" >> ${grun}

gnuplot ${grun}
epstopdf ${epsrun}


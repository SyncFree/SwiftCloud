#!/bin/bash
name=result-social
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

echo "set style data histograms" >> ${grun}
#echo "unset xtics"  >> ${grun}
#echo "set xtics nomirror rotate by -45 scale 0 "  >> ${grun}
echo "set key outside right top vertical Left reverse noenhanced autotitles columnhead nobox" >> ${grun}


echo "set yrange [0:]" >> ${grun}
echo "set style histogram rowstacked title  offset character 0, 0, 0" >> ${grun} 
echo "set style fill solid 1.00 border lt -1" >> ${grun}
echo "set boxwidth 0.75" >> ${grun}

echo "i=5" >> ${grun}
echo -n "plot \"${data}\" using 3:xtic(10), for [i=4:8] '' using i" >> ${grun}

gnuplot ${grun}
epstopdf ${epsrun}


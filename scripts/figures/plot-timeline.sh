#!/bin/bash
data1=social-timeline-cached.txt
data2=social-timeline-notcached.txt

name=social-session
grun=${name}-timeline.gp
epsrun=${name}-timeline.eps


echo "set term postscript eps enhanced color 22" > ${grun}
echo "set output \"${epsrun}\"" >> ${grun}
echo "set size 1.1,1" >> ${grun}
echo "set xlabel \"Setting\"" >> ${grun}
echo "set ylabel \"Time [ms]\"" >> ${grun}
#echo "set pointsize 3" >> ${grun}
#echo "set style data histograms" >> ${grun}
echo "unset xtics"  >> ${grun}
#echo "set xtics nomirror rotate by -45 scale 2 font \",18\""  >> ${grun}

echo "set yrange [0:]" >> ${grun}
#echo "set style histogram errorbars linewidth 3 gap 4" >> ${grun} 

echo -n "plot \"${data1}\" every ::3::500 using 5:4 with lines,\"${data2}\" every ::3::500 using 5:4 with lines" >> ${grun}

gnuplot ${grun}
epstopdf ${epsrun}


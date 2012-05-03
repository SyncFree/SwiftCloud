#!/bin/bash
name=social-responsiveness-config-RR-result-social
data=$name-stats.txt

grun=${name}-time.gp
epsrun=${name}-time.eps


echo "set term postscript eps enhanced color 22" > ${grun}
echo "set output \"${epsrun}\"" >> ${grun}
echo "set size 1.1,1" >> ${grun}
echo "set xlabel \"Setting\"" >> ${grun}
echo "set ylabel \"Time [ms]\"" >> ${grun}
echo "set pointsize 3" >> ${grun}
echo "set style data histograms" >> ${grun}
echo "unset xtics"  >> ${grun}
echo "set xtics nomirror rotate by -45 scale 2 font \",18\""  >> ${grun}

echo "set yrange [0:]" >> ${grun}
echo "set style histogram errorbars linewidth 3 gap 4" >> ${grun} 

echo "set boxwidth 2.00 relative" >> ${grun}

echo "set style fill solid border -1" >> ${grun}
echo "set bars front" >> ${grun}
echo -n "plot \"${data}\" every ::2 using 1:2:xtic(10) notitle" >> ${grun}

gnuplot ${grun}
epstopdf ${epsrun}


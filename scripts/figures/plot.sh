#!/bin/bash
name=result-runping
data=$name-stats.txt

grun=${name}-time.gp
epsrun=${name}-time.eps

echo "set term postscript eps enhanced color 22" > ${grun}
echo "set output \"${epsrun}\"" >> ${grun}
echo "set size 1,1.1" >> ${grun}
echo "set xlabel \"Setting\"" >> ${grun}
echo "set ylabel \"Time [s]\"" >> ${grun}
echo "set pointsize 3" >> ${grun}

echo "set style data histograms" >> ${grun}
echo "unset xtics"  >> ${grun}
echo "set xtics nomirror rotate by -45 scale 0 "  >> ${grun}

echo "set yrange [0:]" >> ${grun}
echo "set style histogram errorbars linewidth 3 gap 1" >> ${grun} 
echo "set style fill solid 0.3 border -1" >> ${grun}
echo  "set bars front" >> ${grun}
echo -n "plot \"${data}\" using 2:5:6:xtic(1) notitle" >> ${grun}

gnuplot ${grun}
epstopdf ${epsrun}


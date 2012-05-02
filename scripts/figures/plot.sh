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
echo "set boxwidth 1 relative" >> ${grun}

echo "set style data histogram" >> ${grun}
echo "set yrange [0:]" >> ${grun}
echo "set style fill solid 1.0 border -1" >> ${grun}

echo -n "plot \"${data}\" using 2:xticlabels(1) notitle" >> ${grun}

gnuplot ${grun}
epstopdf ${epsrun}


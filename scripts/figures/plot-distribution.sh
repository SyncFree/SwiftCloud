#!/bin/bash
name=social-session-distribution
data=result-social-cummulative.txt

grun=${name}-time.gp
epsrun=${name}-time.eps


echo "set term postscript eps enhanced color 22" > ${grun}
echo "set output \"${epsrun}\"" >> ${grun}
echo "set size 1.1,1" >> ${grun}
echo "set key left" >> ${grun}
echo "set xlabel \"Duration [ms]\"" >> ${grun}
echo "set ylabel \"% of operations\"" >> ${grun}
echo "set pointsize 3" >> ${grun}
echo "set style data histograms" >> ${grun}
echo "unset xtics"  >> ${grun}
echo "set xtics nomirror rotate by -45 scale 2 font \",18\""  >> ${grun}

echo "set yrange [0:1]" >> ${grun}
#echo "set style histogram errorbars linewidth 3 gap 4" >> ${grun} 
echo "set style histogram cluster gap 1" >>${grun}
echo "set boxwidth 1.00 relative" >> ${grun}

echo "set style fill solid border -1" >> ${grun}
echo "set bars front" >> ${grun}
echo -n "plot \"${data}\" using 2 t \"STATUS\" fs pattern 1, '' using 2 t \"POST\" fs pattern 2,'' using 3 t \"FRIEND\" fs pattern 5, '' using 2 t \"READ\" fs pattern 4,'' using 5:xtic(1) t \"SEE-FRIENDS\" " >> ${grun}




gnuplot ${grun}
epstopdf ${epsrun}


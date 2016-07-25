grep Up out/*.log | awk -F',|:' '{x[$1]=$0} END {for (i in x) print x[i]}' | sort

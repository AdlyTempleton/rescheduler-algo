grep Reschedule out/*.log | awk -F: '{print $1}' | uniq -c


#!/bin/sh 

# collect cpu, memory and disk metrics and save to csv
#
# usage:
#   ./host-metrics-collector.sh
#
# output:
#   host-metrics.csv

# create csv file with date, time, cpu, memory, disk reads, disk writes
echo "date,time,cpu,memory,disk reads,disk writes" >> host-metrics.csv

# loop forever
while true
do
    # get date and time
    date=$(date +%Y-%m-%d)
    time=$(date +%H:%M:%S)
    
    # get cpu usage
    cpu=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/")
    
    # get memory usage
    memory=$(free -m | awk 'NR==2{printf "%s/%sMB (%.2f%%)\n", $3,$2,$3*100/$2 }')
    
    # get disk reads and writes
    disk_reads=$(iostat -d | grep sda | awk '{print $4}')
    disk_writes=$(iostat -d | grep sda | awk '{print $7}')
    
    # write to csv
    echo "$date,$time,$cpu,$memory,$disk_reads,$disk_writes" >> host-metrics.csv
    
    # sleep 10 seconds
    sleep 10
done

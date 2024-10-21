#!/bin/bash

FILE=$1

echo "Latencies"
grep "Latency" ${FILE} | awk '{print $5}'

echo ""
echo "Throughput"
grep "Throughput" ${FILE} | head -n 72 | awk '{print $4}'

echo ""
echo "Host read"
grep "Host_read" ${FILE} | head -n 72 | awk '{print $4}'

echo ""
echo "Host write"
grep "Host_write" ${FILE} | head -n 72 | awk '{print $4}'

echo ""
echo "GC read"
#grep "GC_read" ${FILE} | head -n 72 | awk '{print $4}'
grep -E "GC_read" ${FILE} | awk '{sum += $4} END {print sum}'

echo ""
echo "GC write"
#grep "GC_write" ${FILE} | head -n 72 | awk '{print $4}'
grep -E "GC_write" ${FILE} | awk '{sum += $4} END {print sum}'

echo ""
echo "Comp. read"
#grep "Comp_read" ${FILE} | head -n 72 | awk '{print $4}'
grep -E "Comp_read" ${FILE} | awk '{sum += $4} END {print sum}'

echo ""
echo "Comp. write"
#grep "Comp_write" ${FILE} | head -n 72 | awk '{print $4}'
grep -E "Comp_write" ${FILE} | awk '{sum += $4} END {print sum}'

echo ""
echo "Total write"
grep "_write" ${FILE} | awk '{sum += $4} END {print sum}'

echo ""
echo "Background reads"
grep -E "Comp_read|GC_read" ${FILE} | awk '{sum += $4} END {print sum}'

echo ""
echo "Total erase"
l=`grep -n Latency ${FILE} | head -n 1 | awk -F  ':' '/1/ {print $1}'`
le=`wc -l ${FILE}`
e=`tac ${FILE} | grep "gc_data!" | head -n 1 | awk '{print $3}'`
s=`head -n $l ${FILE} | grep "gc_data!" | tail -n 1 | awk '{print $3}'`
d=$(($e-$s))
e=`tac ${FILE} | grep "gc_meta!" | head -n 1 | awk '{print $3}'`
s=`head -n $l ${FILE} | grep "gc_meta!" | tail -n 1 | awk '{print $3}'`
m=$(($e-$s))
echo $(($d + $m))

echo ""
echo "Avg. throughput"
grep "Throughput" ${FILE} | awk '{sum += $4} END {print sum/NR}'

echo ""
echo "Flash access count"
for i in {0..64};
do
    tac ${FILE} | grep "\[Flash access count: ${i}\]" | head -n1
done

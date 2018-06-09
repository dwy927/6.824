#！ /bin/bash

var=0
total=$2

while [ "$var" -le "$total" ]
do
echo "test number: $var"

go test -run $1

echo "" 
var=$((var + 1))
done

exit 0

#！ /bin/bash

var=0
total=$1

while [ "$var" -le "$total" ]
do
echo "test number: $var"

go test

echo "" 
var=$((var + 1))
done

exit 0

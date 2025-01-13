#!/bin/bash

# This will attempt to create a large array in memory (more than 3.5GB)
# Array with 1 billion (1e9) integers takes about 4GB
echo "Attempting to allocate 4GB of memory..."

arr=()
for ((i=0; i<1000000000; i++)); do
    arr+=($i)
done

echo "Memory allocation successful!"

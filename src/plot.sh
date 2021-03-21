#!/bin/bash

if [ -d "plots" ]; then
    rm -rf "plots"
    mkdir "plots"
else
    mkdir "plots"
fi


./plot1.py
./plot2.py

mv *.png plots
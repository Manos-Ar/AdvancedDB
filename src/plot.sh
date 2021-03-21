#!/bin/bash

if [ -d "plots" ]; then
    rm -rf "plots"
    mkdir "plots"
else
    mkdir "plots"
fi


./plot.py

mv *.png plots
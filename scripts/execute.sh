#!/usr/bin/env bash


python src/coordinator.py -f "res/raposa e as uvas.txt" &
sleep 5
python src/worker.py




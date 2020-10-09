#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from select import select
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer


#cmd = 'ls -al /tmp'
#cmd = 'python /tmp/sdelarue/SLAVE.py'
cmd = 'python ~/Documents/MS_BGD-Telecom_PARIS/INF727_Systemes_repartis/Hadoop_Map_Reduce_from_scratch/SLAVE.py'

timeout=15
process = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
try:
    stdout, stderr = process.communicate(timeout=timeout)
    print(stdout)
except TimeoutExpired:
    process.kill()
    raise TimeoutExpired(process.args, timeout)


'''
process = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)

try:
    timer = Timer(2, process.kill)
    timer.start()
    while process.poll() is None:
        line = process.stdout.readline() # bloque jusqu'à la prochaine ligne affichée dans SLAVE.py
        print(line)
finally:
    timer.cancel()
    if process.poll() != 0:
        raise TimeoutExpired('Command %s timed out ' % cmd)

#(stdout, stderr) = popen.communicate()
'''

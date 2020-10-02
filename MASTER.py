import os
import sys
from subprocess import Popen, PIPE, STDOUT

# source : http://python-simple.com/python-modules-autres/lancement-process.php

#cmd = 'ls -al /tmp'
cmd = 'python /tmp/sdelarue/SLAVE.py'

popen = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
(stdout, stderr) = popen.communicate()

if (popen.returncode == 0):
    print(stdout)
else:
    print(stderr)


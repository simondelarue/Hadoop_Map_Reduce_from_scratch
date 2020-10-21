import time
import sys
import re
from subprocess import PIPE, Popen, STDOUT
import string
import socket
import os


def hash_function(word):
    alphabet = string.ascii_lowercase + string.ascii_uppercase
    return int(''.join([str(alphabet.index(letter)) for letter in word]))


def get_machines(filename):
    machines = []
    with open(filename) as f:
        for line in f.readlines():
            machines.append(line.split('\n')[0])
    return machines


def shuffle(filename, machines):
    with open(filename, encoding='utf8') as f1:
        for line in f1.readlines():
            word = line.split()[0]

            hash_value = hash_function(word)
            hostname = machines[hash_value % len(machines)]

            with open('/tmp/sdelarue/shuffles/' + str(hash_value) + '-' + socket.gethostname() + '.txt', 'a') as f2:
                f2.write(word + ' ' + '1' + '\n')
            f2.close()

            # Creation d'un répertoire shufflesreceived sur machine distante
            direct = '/tmp/sdelarue/shufflesreceived'
            cmd = 'ssh sdelarue@' + hostname + ' mkdir -p ' + direct
            mkdir_process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
            mkdir_process.wait()
            mkdir_process.kill()

            # Copie vers autres workers
            cmd = "scp /tmp/sdelarue/shuffles/" + str(hash_value) + '-' + socket.gethostname() + '.txt' \
                + " sdelarue@" + hostname + ":/tmp/sdelarue/shufflesreceived"
            process_shuffle = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
            process_shuffle.wait()
            process_shuffle.kill()

            #print(hostname  + ' : ' + str(hash_value) + '-' + socket.gethostname() + '.txt')


def map(filename):
    ''' Map each word in file to 1'''

    file_num = re.findall('[0-9]+', filename)[0]
    with open(filename, encoding='utf8') as f1, \
            open('/tmp/sdelarue/maps/' + 'UM' + file_num + '.txt', 'w+') as f2:
        for line in f1.readlines():
            for word in line.split():
                f2.write(word + ' ' + str(1) + '\n')


def main():

    if len(sys.argv) != 3:
        print('usage: ./SLAVE.py mode = {0 | 1} input_path')
        sys.exit(1)

    mode = sys.argv[1]
    filename = sys.argv[2]
    
    # MAP
    if (mode == str(0)):
        # Creation du répertoire maps en local sur la machine SLAVE
        direct = '/tmp/sdelarue/maps'
        cmd = 'mkdir -p ' + direct
        mkdir_process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
        mkdir_process.wait()
        mkdir_process.kill()

        map(filename)

    # SHUFFLE
    elif (mode == str(1)):
        machines = get_machines('/tmp/sdelarue/available_machines.txt')
        # Creation du répertoire shuffles en local sur la machine SLAVE
        direct = '/tmp/sdelarue/shuffles'
        if not os.path.exists(direct):
            os.mkdir(direct)

        shuffle(filename, machines)
        
    else:
        print('unknown mode: ' + mode)
        sys.exit(1)

if __name__ == '__main__':
  main()
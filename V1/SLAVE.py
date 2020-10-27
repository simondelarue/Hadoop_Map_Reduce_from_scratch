import sys
import re
from subprocess import PIPE, Popen, STDOUT
import string
import socket
import os
import hashlib


'''def hash_function(word):
    alphabet = string.ascii_lowercase + string.ascii_uppercase
    return int(''.join([str(alphabet.index(letter)) for letter in word]))'''
def hash_function(word):
    return int(hashlib.sha256(word.encode('utf-8')).hexdigest(), 16) % 10**8


def get_machines(filename):
    machines = []
    with open(filename, encoding='utf8') as f:
        for line in f.readlines():
            machines.append(line.split('\n')[0])
    return machines


def shuffle(filename, machines):
    with open(filename, encoding='utf8') as f1:
        for line in f1.readlines():
            word = line.split()[0]

            hash_value = hash_function(word)
            hostname = machines[hash_value % len(machines)]

            with open(f'/tmp/sdelarue/shuffles/{str(hash_value)}-{socket.gethostname()}.txt', 'a') as f2:
                f2.write(f'{word} 1\n')
                f2.close()

            # Creation d'un répertoire shufflesreceived sur machine distante
            direct = '/tmp/sdelarue/shufflesreceived'
            cmd = f'ssh sdelarue@{hostname} mkdir -p {direct}'
            mkdir_process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
            mkdir_process.wait()
            mkdir_process.kill()

            # Copie vers autres workers
            cmd = f"scp /tmp/sdelarue/shuffles/{str(hash_value)}-{socket.gethostname()}.txt sdelarue@{hostname}:/tmp/sdelarue/shufflesreceived"
            process_shuffle = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
            process_shuffle.wait()
            process_shuffle.kill()

            #print(hostname  + ' : ' + str(hash_value) + '-' + socket.gethostname() + '.txt')


def mapper(filename):
    ''' Map each word in file to 1'''
    file_num = re.findall('[0-9]+', filename)[0]
    with open(filename, encoding='utf8') as f1, \
            open(f'/tmp/sdelarue/maps/UM{file_num}.txt', 'w+') as f2:
        for line in f1.readlines():
            for word in line.split():
                f2.write(word + ' ' + str(1) + '\n')
        f2.close()


def reducer():
    ''' Somme les nombres d'occurences d'un même mot '''

    direct = '/tmp/sdelarue'
    # Parcours des shufflesreceived
    files_hashes = {}
    if os.path.exists(f'{direct}/shufflesreceived'):
        
        for shuffle_received in os.listdir(f'{direct}/shufflesreceived'):
            # Récupération du numéro de hash du shuffle
            hash_value = (re.findall('[0-9]+', shuffle_received.split('-')[0])[0])

            cnt_occurences = 0
            word = ''
            # Compte le nombre de mots d'un fichier de shufflesreceived
            with open(f'{direct}/shufflesreceived/{shuffle_received}', encoding='utf8') as f:
                for line in f.readlines():
                    cnt_occurences += 1
                    word = line.split()[0]

            # Stocke les informations d'un shufflesreceived dans un dictionnaire {valeur de hash : [mot, nb occurences]}
            files_hashes[hash_value] = list(map(lambda x, y: x + y, \
                files_hashes.get(hash_value, [word, 0]), \
                ["", cnt_occurences]))

        # Creation des fichiers reduces.txt
        for key, value in files_hashes.items():
            with open(f'{direct}/reduces/{key}.txt', 'w') as f_reduce:
                f_reduce.write(f"{value[0]} {value[1]}")
                f_reduce.close()
            # Affichage du résultat dans la sortie standard, pour récupération par le MASTER
            print(f"{value[0]} {value[1]}")


def main():

    if len(sys.argv) != 3:
        print('usage: ./SLAVE.py mode = {0 | 1 | 2} {input_path | None}')
        sys.exit(1)

    mode = sys.argv[1]
    filename = sys.argv[2]
    
    # MAP
    if (mode == str(0)):
        # Creation du répertoire maps en local sur la machine SLAVE
        direct = '/tmp/sdelarue/maps'
        cmd = f'mkdir -p {direct}'
        mkdir_process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
        mkdir_process.wait()
        mkdir_process.kill()

        mapper(filename)

    # SHUFFLE
    elif (mode == str(1)):
        machines = get_machines('/tmp/sdelarue/available_machines.txt')
        # Creation du répertoire shuffles en local sur la machine SLAVE
        direct = '/tmp/sdelarue/shuffles'
        if not os.path.exists(direct):
            os.mkdir(direct)
        shuffle(filename, machines)

    # REDUCE
    elif (mode == str(2)):
        # Creation du répertoire reduces en local sur la machine SLAVE
        direct = '/tmp/sdelarue/reduces'
        if not os.path.exists(direct):
            os.mkdir(direct)
        reducer()
        
    else:
        print('unknown mode: ' + mode)
        sys.exit(1)

if __name__ == '__main__':
  main()


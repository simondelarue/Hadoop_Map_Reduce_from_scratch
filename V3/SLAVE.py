import sys
import re
from subprocess import PIPE, Popen, STDOUT
from multiprocessing import Pool
import string
import socket
import os
import hashlib
import time
import collections

# Dictionnaires de données pour l'étape REDUCE
WC_DICT = {}


def get_machines(filename):
    ''' Renvoie la liste des machines contenues dans un fichier '''

    machines = []
    with open(filename, encoding='utf8') as f:
        for line in f.readlines():
            machines.append(line.split('\n')[0])
    return machines


def hash_function(word):
    ''' Fonction de hashage appliquée aux mots des fichiers de données '''

    return int(hashlib.sha256(word.encode('utf-8')).hexdigest(), 16) % 10**8


# --------------------------------------------------------------------------- #
#                               SHUFFLE                                       #
# --------------------------------------------------------------------------- #

def create_shuffle(word):
    ''' Crée un fichier shuffle a envoyer sur le réseau '''

    receiver = machines[hash_function(word) % len(machines)]
    with open(f'/tmp/sdelarue/shuffles/{receiver}_{socket.gethostname()}.txt', 'a') as f:
        f.write(f'{word} ')
    f.close()


def send_shuffle(filename):
    ''' Envoie un fichier shuffle par scp au receiver '''

    cmd = f"scp /tmp/sdelarue/shuffles/{filename} sdelarue@{filename.split('_')[0]}:/tmp/sdelarue/shufflesreceived"
    process_shuffle = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_shuffle.wait()
    process_shuffle.kill()


def shuffle(filename, machines):
    ''' Gestion des transferts de données entre machines du réseau '''

    # Lit les fichiers UM.txt
    with open(filename, encoding='utf8') as f1:
        words = f1.read().split()

    # Calcule le hash du mot et ecrit le résultat dans le fichier correspondant
    with Pool() as p:
        p.map(create_shuffle, words)


# --------------------------------------------------------------------------- #
#                               MAPPER                                        #
# --------------------------------------------------------------------------- #

def mapper(filename):
    ''' Map chaque mot d'un fichier split et l'écrit dans un fichier UM.txt '''

    file_num = re.findall('[0-9]+', filename)[0]
    with open(filename, encoding='utf8') as f1, \
            open(f'/tmp/sdelarue/maps/UM{file_num}.txt', 'w+') as f2:
        for line in f1.readlines():
            for word in line.split():
                f2.write(f'{word} ')
        f2.close()


# --------------------------------------------------------------------------- #
#                               REDUCER                                       #
# --------------------------------------------------------------------------- #

def reducer():
    ''' Somme les nombres d'occurences d'un même mot '''
    
    direct = '/tmp/sdelarue'
    # Parcours des shufflesreceived
    if os.path.exists(f'{direct}/shufflesreceived'):
        for shuffle_received in os.listdir(f'{direct}/shufflesreceived'):
            # Compte le nombre de mots d'un fichier de shufflesreceived
            with open(f'{direct}/shufflesreceived/{shuffle_received}', encoding='utf8') as f:
                readed_shuffle = f.read()
            for word in readed_shuffle.split(' '):
                WC_DICT[word] = WC_DICT.get(word, 0) + 1

        # Ecrit l'agrégation des résultats dans un fichier reduce
        for key, value in WC_DICT.items():
            print(f'{key} {value}')


def main():

    if len(sys.argv) != 3:
        print('usage: ./SLAVE.py mode = {0 | 1 | 2 | 3} {input_path | None}')
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
        global machines
        machines = get_machines('/tmp/sdelarue/available_machines.txt')
        # Creation du répertoire shuffles en local sur la machine SLAVE
        direct = '/tmp/sdelarue/shuffles'
        if not os.path.exists(direct):
            os.mkdir(direct)
        shuffle(filename, machines)

    elif (mode == str(3)):
        if os.path.exists(f'/tmp/sdelarue/shuffles'):
            with Pool() as p:
                p.map(send_shuffle, os.listdir('/tmp/sdelarue/shuffles'))

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
import sys
import re
from subprocess import PIPE, Popen, STDOUT
from multiprocessing import Pool
import string
import socket
import os
import hashlib

# Dictionnaires de données pour les étapes de SHUFFLE et REDUCE
SHUFFLE_DICT = {}
WC_DICT = {}


def hash_function(word):
    ''' Fonction de hashage appliquée aux mots des fichiers de données '''
    return int(hashlib.sha256(word.encode('utf-8')).hexdigest(), 16) % 10**8


def get_machines(filename):
    ''' Renvoie la liste des machines contenues dans un fichier '''
    machines = []
    with open(filename, encoding='utf8') as f:
        for line in f.readlines():
            machines.append(line.split('\n')[0])
    return machines


def create_shuffle(receiver):
    ''' Ecrit un dictionnaire de données dans un fichier .txt '''

    with open(f'/tmp/sdelarue/shuffles/{receiver}_{socket.gethostname()}.txt', 'a') as f:
        for value in SHUFFLE_DICT.get(receiver):
            f.write(f'{value} 1\n')
        f.close()
    print(f'SHUFFLE CREATED {receiver}')


def send_shuffle(filename):
    ''' Envoie un fichier shuffle par scp au receiver '''

    cmd = f"scp /tmp/sdelarue/shuffles/{filename} sdelarue@{filename.split('_')[0]}:/tmp/sdelarue/shufflesreceived"
    process_shuffle = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_shuffle.wait()
    process_shuffle.kill()
    print(f'SHUFFLE SENT {filename}')


def create_reduce(word):
    ''' Ecrit un dictionnaire de données dans un fichier .txt '''
    with open(f'/tmp/sdelarue/reduces/reduce.txt', 'a') as f_reduce:
        f_reduce.write(f"{word} {WC_DICT.get(word)}\n")
        # Affichage du résultat dans la sortie standard, pour récupération par le MASTER
        print(f"{word} {WC_DICT.get(word)}")
    f_reduce.close()

    



def shuffle(filename, machines):
    ''' Gestion des transferts de données entre machines du réseau '''

    # Lit les fichiers UM.txt et stocke les données dans un dictionnaire
    with open(filename, encoding='utf8') as f1:
        for line in f1.readlines():
            word = line.split()[0]

            # Application de la fonction de hashage à chaque mot
            hash_value = hash_function(word)
            receiver = machines[hash_value % len(machines)]
            SHUFFLE_DICT[receiver] = SHUFFLE_DICT.get(receiver, []) + [word]
    
    # Ecrit les données du dictionnaire dans plusieurs fichiers .txt
    with Pool() as p:
        p.map(create_shuffle, list(SHUFFLE_DICT.keys()))
    # Envoie les données aux autres workers du réseau
    with Pool() as p:
        p.map(send_shuffle, os.listdir('/tmp/sdelarue/shuffles'))


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
    if os.path.exists(f'{direct}/shufflesreceived'):
        for shuffle_received in os.listdir(f'{direct}/shufflesreceived'):
            # Compte le nombre de mots d'un fichier de shufflesreceived
            with open(f'{direct}/shufflesreceived/{shuffle_received}', encoding='utf8') as f:
                for line in f.readlines():
                    word = line.split()[0]
                    WC_DICT[word] = WC_DICT.get(word, 0) + 1

        # Ecrit l'agrégation des résultats dans un fichier reduce
        with Pool() as p:
            p.map(create_reduce, list(WC_DICT.keys()))


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




'''def reducer():
   

    direct = '/tmp/sdelarue'
    # Clé = mot, valeurs = nb occurences du mot
    #wc_dict = {}
    
    # Parcours des shufflesreceived
    if os.path.exists(f'{direct}/shufflesreceived'):
        for shuffle_received in os.listdir(f'{direct}/shufflesreceived'):

            # Compte le nombre de mots d'un fichier de shufflesreceived
            with open(f'{direct}/shufflesreceived/{shuffle_received}', encoding='utf8') as f:
                for line in f.readlines():
                    word = line.split()[0]
                    wc_dict[word] = wc_dict.get(word, 0) + 1

        # Lit les données shufflereceived dans les différents fichiers et stocke les résulats dans un dictionnaire
        with Pool() as p:
            p.map(create_reduce, os.listdir('/tmp/sdelarue/shufflesreceived'))
        # Ecrit l'agrégation des résultats dans un fichier reduce
        with Pool() as p:
            p.map(create_reduce, list(WC_DICT.keys()))

        # Creation du fichier reduce.txt
        with open(f'{direct}/reduces/reduce.txt', 'w') as f_reduce:
            for key, value in wc_dict.items():
                f_reduce.write(f"{key} {value}")
                # Affichage du résultat dans la sortie standard, pour récupération par le MASTER
                print(f"{key} {value}")
            f_reduce.close()'''
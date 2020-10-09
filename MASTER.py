#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from select import select
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer



def ssh(machine, filename):
    ''' 
    Objectif
        Teste une connexion ssh sur une machine passée en paramètre.
        Si la connexion est un succès, certains traitements sont effectués sur la machine distante.
        Si le temps de connexion dépasse un timeout, le processus est killé.
    input
        machine : Nom de la machine distante
        filename : Nom du fichier à copier sur la machine distante '''

    cmd = 'ssh ' + machine + ' hostname'
    timeout = 8
    # Processus de connexion ssh sur machine distante
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)

    try:
        # La sortie d'erreur est redirigée sur la sortie standard
        # Si la connexion réussi avant le timeout, on appelle la commande 'hostname' sur la machine distante
        stdout, stderr = process.communicate(timeout=timeout)
        print(str(stdout).strip() + ' : connexion OK')
        
        # Processus de création de répertoire sur la machine distante
        direct = '/tmp/sdelarue/splits'
        print('Creation du répertoire : ', direct)
        mkdir_process = Popen(mkdir_cmd(machine, direct), shell=True)
        mkdir_process.wait()
        print('Repertoire ' + direct + ' créé')

        # Processus de copie de fichier sur la machine distante
        file_to_copy = filename
        print('Copy du fichier : ', file_to_copy)
        sshcopy_file_process = Popen(sshcopy_file_cmd(machine, file_to_copy, direct, direct), shell=True)
        sshcopy_file_process.wait()

    except TimeoutExpired:
        # Si le timeout est dépassé, kill du processus de connexion ssh
        process.kill()
        raise TimeoutExpired(process.args, timeout)


def mkdir_cmd(machine, directory):
    ''' input
            machine : Nom de la machine distante
            directory : Chemin complet du répertoire à créer 
        output
            String : Commande shell pour la création du répertoire '''
    return ('ssh ' + machine + ' mkdir -p ' + directory)


def sshcopy_file_cmd(machine, filename, from_directory, to_directory):
    ''' input
            machine : Nom de la machine distante
            filename : Nom du fichier à copier
            from_directory : Chemin complet du répertoire contenant le fichier 'filename'
            to_directory : Chemin complet du répertoire dans lequel copier le fichier 'filename'
        output
            String : Commande shell pour la copie d'un fichier sur machine distante via ssh '''
    return ('scp ' + from_directory + '/' + filename + ' ' + machine + ':' + to_directory + '/' + filename)


def splits_files(direct):
    ''' input
            direct : chemin du repertoire contenant les fichiers splits
        output
            liste des noms des fichiers de splits '''
    files = os.listdir(direct)
    return files

def ssh_connect_and_task(filename):
    ''' Objectif
            Lit un fichier contenant la liste des noms de machines et appelle la fonction ssh()
            sur chacune d'elles 
        input
            Nom du fichier contenant la liste des machines '''

    splits = splits_files('/tmp/sdelarue/splits')

    with open(filename, encoding='utf8') as f:

        # parcours de la liste des machines à utiliser
        for line in f.readlines():

            if len(splits) > 0: # test s'il reste des splits de données à deployer
                split_name = splits[-1]
                print('\nTest de connexion sur la machine : ' + line.strip())
                ssh(line.strip(), split_name)
                splits.pop()

    f.close()


ssh_connect_and_task('machines.txt')

'''
Etape 9 
#cmd = 'ls -al /tmp'
connect_ssh_cmd = 'ssh sdelarue@tp-1a207-15'
runpy_cmd = 'python /tmp/sdelarue/SLAVE.py'
#cmd = 'python ~/Documents/MS_BGD-Telecom_PARIS/INF727_Systemes_repartis/Hadoop_Map_Reduce_from_scratch/SLAVE.py'

timeout=10
process = Popen(connect_ssh_cmd + ' ' + runpy_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
try:
    stdout, stderr = process.communicate(timeout=timeout)
    print(stdout)
except TimeoutExpired:
    process.kill()
    raise TimeoutExpired(process.args, timeout)
'''
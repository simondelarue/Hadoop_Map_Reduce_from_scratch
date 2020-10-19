#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import sys
from select import select
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer

from DEPLOY import deploy
from ssh_connexions_utils import *

# True si le déploiement des SLAVE est fait sur les machines
DEPLOY_DONE = False

# Machines exploitables
MACHINES_OK = []

# Informe de la répartition des données sur les différentes machines
FILE_DISTRIBUTION = {}


def ssh(machine, filename):

    direct = '/tmp/sdelarue/splits'
    file_to_copy = filename

    connect_cmd = "ssh -o \'StrictHostKeyChecking=no\' " + machine
    mkdir_cmd = ' mkdir -p ' + direct + ' ; '
    sshcopy_file_process_cmd = sshcopy_file_cmd(machine, file_to_copy, direct, direct)
    cmd = connect_cmd + mkdir_cmd + sshcopy_file_process_cmd

    timeout = 10
    # Processus de connexion ssh sur machine distante
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process.wait()

    try:
        # La sortie d'erreur est redirigée sur la sortie standard
        stdout, stderr = process.communicate(timeout=timeout)
        
        if stderr == "":
            # Processus de création de répertoire sur la machine distante
            
            #mkdir_process = Popen(mkdir_cmd(machine, direct), shell=True, text=True)
            #mkdir_process.wait()

            # Processus de copie de fichier sur la machine distante
            
            #sshcopy_file_process = Popen(sshcopy_file_cmd(machine, file_to_copy, direct, direct), shell=True, text=True)
            #sshcopy_file_process.wait()

            process.kill()
            print(f"Connexion machine : {machine} : OK | données {file_to_copy} copiées")
        else:
            process.kill()
            print(f"Connexion machine : {machine} -> Echec | données non copiées")

    except TimeoutExpired:
        # Si le timeout est dépassé, kill du processus de connexion ssh
        process.kill()
        print(f"Connexion machine : {machine} : Echec TimeOut | données non copiées")


def ssh_connect_and_split(filename):

    if (DEPLOY_DONE == False):
        # Liste des machines sur lesquelles travailler
        machines = read_machines(filename)

        # Deploy slaves
        MACHINES_OK = deploy(machines)

        DEPLOY_DONE == True


    splits = splits_files('/tmp/sdelarue/splits')

    print(' Schéma de partition des données ')

    FILE_DISTRIBUTION = map_files_machines(splits, MACHINES_OK)
    print_data_repartition(FILE_DISTRIBUTION)

    print('---------------------------------------------------')
    print(' Copie des données')
    for split_name, machine in FILE_DISTRIBUTION.items():
        ssh(machine, split_name)

    '''index = 0
    while len(splits) > 0: # tant qu'il reste des splits de données à deployer
        split_name = splits[-1]
        ssh(machines_list[index], split_name)
        index += 1
        splits.pop()'''




# Deploy données splitées sur machines disponibles
#ssh_connect_and_split(machines_OK)
ssh_connect_and_split("machines.txt")



'''
Etape 9 
#cmd = 'ls -al /tmp'
connect_ssh_cmd = 'ssh sdelarue@tp-1a207-15'
runpy_cmd = 'python /tmp/sdelarue/SLAVE.py'

timeout=10
process = Popen(connect_ssh_cmd + ' ' + runpy_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
try:
    stdout, stderr = process.communicate(timeout=timeout)
    print(stdout)
except TimeoutExpired:
    process.kill()
    raise TimeoutExpired(process.args, timeout)
'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import sys
from select import select
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer
import re

from multiprocessing import Pool

from DEPLOY import deploy
from ssh_connexions_utils import splits_files, sshcopy_file_cmd, read_machines, map_files_machines, \
    print_data_repartition, getName


# True si le déploiement des SLAVE est fait sur les machines
DEPLOY_DONE = False

# Machines exploitables
MACHINES_OK = []

# Informe de la répartition des données sur les différentes machines
FILE_DISTRIBUTION = {}


def deploy_machines(machine):
    direct = '/tmp/sdelarue'
    file_to_copy = 'available_machines.txt '
    timeout = 10
    # Déploiement de la liste de machines sur les workers
    sshcopy_file_process_cmd = 'scp ' + direct + '/' + file_to_copy + getName() + machine + ':' + direct
    cmd = sshcopy_file_process_cmd
    process_machine = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_machine.wait()
    # Test si TimeOut
    try:
        _, stderr = process_machine.communicate(timeout=timeout)
        if stderr == "":
            process_machine.kill()
            return(f"Machine : {machine} | {file_to_copy} copié")
        else:
            process_machine.kill()
            return(f"Machine : {machine} | {file_to_copy} Echec copie")
    except TimeoutExpired:
        process_machine.kill()
        return(f"Connexion machine : {machine} | {file_to_copy} Echec TimeOut - Echec copie")    


def deploy_data_splits(machine, filename):
    direct = '/tmp/sdelarue/splits'
    file_to_copy = filename
    timeout = 10
    # Déploiement des splits de données
    connect_cmd = "ssh -o \'StrictHostKeyChecking=no\' " + getName() + machine
    mkdir_cmd = ' mkdir -p ' + direct + ' ; '
    sshcopy_file_process_cmd = sshcopy_file_cmd(machine, file_to_copy, direct, direct)
    cmd = connect_cmd + mkdir_cmd + sshcopy_file_process_cmd
    process_splits = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_splits.wait()
    # Test si TimeOut
    try:
        _, stderr = process_splits.communicate(timeout=timeout)
        if stderr == "":
            process_splits.kill()
            return(f"Connexion machine : {machine} : OK | données {file_to_copy} copiées")
        else:
            process_splits.kill()
            return(f"Connexion machine : {machine} -> Echec | données non copiées")
    except TimeoutExpired:
        process_splits.kill()
        return(f"Connexion machine : {machine} : Echec TimeOut | données non copiées")


def launch_map(machine, filename):
    direct = '/tmp/sdelarue'
    file_to_copy = filename
    timeout = 10
    # Lancement du Map
    connect_cmd = "ssh -o \'StrictHostKeyChecking=no\' " + getName() + machine
    lauch_map_cmd = ' python3 ' + direct + '/SLAVE.py' + ' 0 '  + direct + '/splits/' + filename
    cmd = connect_cmd + lauch_map_cmd
    process_map = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_map.wait()
    # Test si TimeOut
    try:
        _, stderr = process_map.communicate(timeout=timeout)
        if stderr == "":
            process_map.kill()
            return(f"Machine : {machine} | MAP {file_to_copy} OK")
        else:
            process_map.kill()
            return(f"Machine : {machine} | MAP {file_to_copy} Echec")
    except TimeoutExpired:
        process_map.kill()
        return(f"Machine : {machine} | MAP {file_to_copy} TimeOut Echec")


def lauch_shuffle(machine, filename):
    direct = '/tmp/sdelarue'
    file_UM = 'UM' + re.findall('[0-9]+', filename)[0] + '.txt'
    timeout = 10
    # Lancement du Shuffle
    connect_cmd = "ssh -o \'StrictHostKeyChecking=no\' " + getName() + machine
    lauch_shuffle_cmd = ' python3 ' + direct + '/SLAVE.py' + ' 1 '  + direct + '/maps/' + file_UM
    cmd = connect_cmd + lauch_shuffle_cmd
    process_shuffle = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_shuffle.wait()
    # Test si TimeOut
    try:
        _, stderr = process_shuffle.communicate(timeout=timeout)
        if stderr == "":
            process_shuffle.kill()
            return(f"Machine : {machine} | SHUFFLE {file_UM} OK")
        else:
            process_shuffle.kill()
            return(f"Machine : {machine} | SHUFFLE {file_UM} Echec")
    except TimeoutExpired:
        process_shuffle.kill()
        return(f"Machine : {machine} | SHUFFLE {file_UM} TimeOut Echec")


def main(filename):
    if (DEPLOY_DONE == False):
        # Liste des machines sur lesquelles travailler
        machines = read_machines(filename)
        # Deploy slaves
        MACHINES_OK = deploy(machines)
        DEPLOY_DONE == True


    # File Distribution
    splits = splits_files('/tmp/sdelarue/splits')
    FILE_DISTRIBUTION = map_files_machines(splits, MACHINES_OK)
    print(' Schema de partition des données ')
    print_data_repartition(FILE_DISTRIBUTION)


    print('---------------------------------------------------')
    print(' Copie des données sur workers')
    with Pool() as p:
        log = p.starmap(deploy_data_splits, zip(list(FILE_DISTRIBUTION.values()), list(FILE_DISTRIBUTION.keys())))   
    for elem in log:
       print(elem)
    

    print('---------------------------------------------------')
    print(' Copie de la liste des machines sur les workers')
    with Pool() as p:
        log_deploy_machines = p.map(deploy_machines, set(list(FILE_DISTRIBUTION.values())))
    for elem in log_deploy_machines:
       print(elem)


    print('===================================================')
    print(' Map ')
    with Pool() as p:
        log_map = p.starmap(launch_map, zip(list(FILE_DISTRIBUTION.values()), list(FILE_DISTRIBUTION.keys())))   
    for elem in log_map:
       print(elem)
    print(' Map finished')


    print('===================================================')
    print(' Shuffle ')
    with Pool() as p:
        log_shuffle = p.starmap(lauch_shuffle, zip(list(FILE_DISTRIBUTION.values()), list(FILE_DISTRIBUTION.keys())))   
    for elem in log_shuffle:
       print(elem)
    print(' Shuffle finished')

    # Sequentiel
    #for split_name, machine in FILE_DISTRIBUTION.items():
    #    ssh(machine, split_name)



# Deploy données splitées sur machines disponibles
if __name__ == '__main__':
  main("/tmp/sdelarue/machines.txt")

#ssh_connect_and_split(






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
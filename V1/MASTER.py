#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
from select import select
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer
import re

from multiprocessing import Pool

from termcolor import colored
import time

from DEPLOY import deploy
from ssh_connexions_utils import list_files_from_dir, sshcopy_file_cmd, read_machines, map_files_machines, \
    print_data_repartition, getName


def sort_results(filename):
    wc = {}
    with open(f'/tmp/sdelarue/result/{filename}') as f:
        for line in f.readlines():
            wc[line.split(' ')[0]] = int(line.split(' ')[1])
    sorted_wc = sorted(wc.items(), key=lambda x: (-x[1], x[0]), reverse=False)

    for elem in sorted_wc:
        with open(f'/tmp/sdelarue/result/sorted_result.txt', 'a') as f_res:
            f_res.write(f"{elem[0]} {str(elem[1])}\n")
        f_res.close()


def create_dir(directory):
    ''' Creation répertoire / suppression auparavant si existe déjà '''

    if os.path.exists(directory):
        p = Popen(f"rm -rf {directory}/", shell=True, stdout=PIPE, stderr=PIPE, text=True)
        p.wait()
        p.kill()
    os.mkdir(directory)


def create_splits(filename, text_window):
    ''' Crée des splits de données à partir d'un fichier txt '''

    with open(filename, encoding='utf8') as f:
        readed_file = f.read()

    splits = []
    file_len = len(readed_file)
    split_start = 0

    # Parcours du fichier txt par fenêtre de taille 'split_size'
    while file_len > text_window:
        split_end = split_start + text_window

        # On cherche l'espace le plus proche
        while readed_file[split_end] != " ":
            split_end -= 1
        splits.append(readed_file[split_start:split_end])
        file_len -= split_end - split_start
        split_start = split_end

    splits.append(readed_file[split_start:len(readed_file)])

    return splits


def write_split(split_liste, directory):
    ''' Ecrit chaque élément d'une liste dans un fichier '''

    for index, split in enumerate(split_liste):
        with open(f'{directory}/S{str(index)}.txt', 'w') as f:
            f.write(split)
            f.close()


def deploy_machines(machine):
    direct = '/tmp/sdelarue'
    file_to_copy = 'available_machines.txt'
    timeout = 10
    # Déploiement de la liste de machines sur les workers
    cmd = f'scp {direct}/{file_to_copy} {getName()}{machine}:{direct}'
    process_machine = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_machine.wait()
    # Test si TimeOut
    try:
        _, stderr = process_machine.communicate(timeout=timeout)
        if stderr == "":
            process_machine.kill()
            return(f"Machine : {machine} | copie {file_to_copy} {colored('OK', 'green')}")
        else:
            process_machine.kill()
            return(f"Machine : {machine} | copie {file_to_copy} {colored('Echec', 'red')}")
    except TimeoutExpired:
        process_machine.kill()
        return(f"Connexion machine : {machine} | copie {file_to_copy} {colored('TimeOut Echec', 'red')}")    


def deploy_data_splits(machine, filename):
    direct = '/tmp/sdelarue/splits'
    file_to_copy = filename
    timeout = 10
    # Déploiement des splits de données
    connect_cmd = f"ssh -o \'StrictHostKeyChecking=no\' {getName()}{machine}"
    mkdir_cmd = f' mkdir -p {direct} ;'
    sshcopy_file_process_cmd = sshcopy_file_cmd(machine, file_to_copy, direct, direct)
    cmd = connect_cmd + mkdir_cmd + sshcopy_file_process_cmd
    process_splits = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_splits.wait()
    # Test si TimeOut
    try:
        _, stderr = process_splits.communicate(timeout=timeout)
        if stderr == "":
            process_splits.kill()
            return(f"Connexion machine : {machine} | données {file_to_copy} copiées {colored('OK', 'green')}")
        else:
            process_splits.kill()
            return(f"Connexion machine : {machine} | données {file_to_copy} non copiées {colored('Echec', 'red')}")
    except TimeoutExpired:
        process_splits.kill()
        return(f"Connexion machine : {machine} | données {file_to_copy} non copiées {colored('TimeOut Echec', 'red')}")


def launch_map(machine, filename):
    direct = '/tmp/sdelarue'
    file_to_copy = filename
    timeout = 10000
    # Lancement du Map
    connect_cmd = f"ssh -o \'StrictHostKeyChecking=no\' {getName()}{machine}"
    lauch_map_cmd = f' python3 {direct}/SLAVE.py 0 {direct}/splits/{filename}'
    cmd = connect_cmd + lauch_map_cmd
    process_map = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_map.wait()
    # Test si TimeOut
    try:
        _, stderr = process_map.communicate(timeout=timeout)
        if stderr == "":
            process_map.kill()
            return(f"Machine : {machine} | MAP {file_to_copy} {colored('OK', 'green')}")
        else:
            process_map.kill()
            return(f"Machine : {machine} | MAP {file_to_copy} {colored('Echec', 'red')}")
    except TimeoutExpired:
        process_map.kill()
        return(f"Machine : {machine} | MAP {file_to_copy} {colored('TimeOut Echec', 'red')}")


def lauch_shuffle(machine, filename):
    direct = '/tmp/sdelarue'
    file_UM = 'UM' + re.findall('[0-9]+', filename)[0] + '.txt'
    timeout = 10000
    # Lancement du Shuffle
    connect_cmd = f"ssh -o \'StrictHostKeyChecking=no\' {getName()}{machine}"
    lauch_shuffle_cmd = f' python3 {direct}/SLAVE.py 1 {direct}/maps/{file_UM}'
    cmd = connect_cmd + lauch_shuffle_cmd
    process_shuffle = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_shuffle.wait()
    # Test si TimeOut
    try:
        _, stderr = process_shuffle.communicate(timeout=timeout)
        if stderr == "":
            process_shuffle.kill()
            return(f"Machine : {machine} | SHUFFLE {file_UM} {colored('OK', 'green')}")
        else:
            print(stderr)
            process_shuffle.kill()
            return(f"Machine : {machine} | SHUFFLE {file_UM} {colored('Echec', 'red')}")
    except TimeoutExpired:
        process_shuffle.kill()
        return(f"Machine : {machine} | SHUFFLE {file_UM} {colored('TimeOut Echec', 'red')}")


def lauch_reduce(machine):
    direct = '/tmp/sdelarue'
    timeout = 10000
    # Lancement du Reduce
    connect_cmd = f"ssh -o \'StrictHostKeyChecking=no\' {getName()}{machine}"
    lauch_reduce_cmd = f' python3 {direct}/SLAVE.py 2 None'
    cmd = connect_cmd + lauch_reduce_cmd
    process_reduce = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_reduce.wait()
    # Test si TimeOut
    try:
        stdout, stderr = process_reduce.communicate(timeout=timeout)
        if stderr == "":
            # Agrégation des reduces des SLAVES dans un fichier result
            with open(f'{direct}/result/result.txt', 'a') as f_result:
                f_result.write(f'{stdout}')
                f_result.close()
            process_reduce.kill()
            return(f"Machine : {machine} | REDUCE {colored('OK', 'green')}")
        else:
            process_reduce.kill()
            return(f"Machine : {machine} | REDUCE {colored('Echec', 'red')}")
    except TimeoutExpired:
        process_reduce.kill()
        return(f"Machine : {machine} | REDUCE {colored('TimeOut Echec', 'red')}")


def lauch_cleaning():
    timeout = 10
    process_cleaning = Popen('python3 CLEAN.py', shell=True, stdout=PIPE, stderr=PIPE, text=True)
    process_cleaning.wait()
    # Test si TimeOut
    try:
        stdout, stderr = process_cleaning.communicate(timeout=timeout)
        if stderr == "":
            print(stdout)
            process_cleaning.kill()
        else:
            print(stderr)
            process_cleaning.kill()
    except TimeoutExpired:
        process_cleaning.kill()



def main(filename):

    # True si le déploiement des SLAVE est fait sur les machines
    DEPLOY_DONE = False
    # Machines exploitables
    MACHINES_OK = []
    # Informe de la répartition des données sur les différentes machines
    FILE_DISTRIBUTION = {}

    if len(sys.argv) > 1 and sys.argv[1] == '-clean':
        # CLEANING
        lauch_cleaning() 

        # DEPLOY slaves
        machines = read_machines(filename)
        MACHINES_OK = deploy(machines)
        DEPLOY_DONE == True

    else:
        with open('/tmp/sdelarue/available_machines.txt', encoding='utf8') as f:
            for line in f.readlines():
                MACHINES_OK.append(line.split('\n')[0])


    # =========================================================
    input_file = sys.argv[2]
    splits_directory = '/tmp/sdelarue/splits'
    result_directory = '/tmp/sdelarue/result'
    text_window = 1000 # nb characters of text windows
    # =========================================================

    # Creation des splits de données
    data_splits = create_splits(f'input/{input_file}', text_window)
    create_dir(splits_directory)
    write_split(data_splits, splits_directory)
    splits = list_files_from_dir(splits_directory)


    print('===================================================')
    print(f' SPLITS | Partition des données {input_file}')
    FILE_DISTRIBUTION = map_files_machines(splits, MACHINES_OK)
    print_data_repartition(FILE_DISTRIBUTION)


    print('---------------------------------------------------')
    print(' SPLITS | Deploiement')
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
    print(' MAP ')
    start = time.time()
    with Pool() as p:
        log_map = p.starmap(launch_map, zip(list(FILE_DISTRIBUTION.values()), list(FILE_DISTRIBUTION.keys())))   
    end = time.time()
    for elem in log_map:
        print(elem)
    time_MAP = (end - start)
    print(f' MAP finished : {time_MAP:.5f} secondes')


    print('===================================================')
    print(' SHUFFLE ')
    start = time.time()
    with Pool() as p:
        log_shuffle = p.starmap(lauch_shuffle, zip(list(FILE_DISTRIBUTION.values()), list(FILE_DISTRIBUTION.keys())))   
    end = time.time()
    for elem in log_shuffle:
        print(elem)
    time_SHUFFLE = (end - start)
    print(f' SHUFFLE finished : {time_SHUFFLE:.5f} secondes')


    print('===================================================')
    print(' REDUCE ')
    create_dir(result_directory)
    start = time.time()
    with Pool() as p:
        log_reduce = p.map(lauch_reduce, set(list(FILE_DISTRIBUTION.values())))   
    end = time.time()
    for elem in log_reduce:
        print(elem)
    time_REDUCE = (end - start)
    print(f' REDUCE finished : {time_REDUCE:.5f} secondes')


    if (len(sys.argv) > 3 and (sys.argv[3] == '-sort')):
        print('===================================================')
        print(" SORT RESULT (see \'sorted_result.txt\') ")
        start = time.time()
        sort_results('result.txt')
        end = time.time()
        time_SORT = (end - start)
        print(f' SORT finished : {time_SORT:.5f} secondes')

        print(f'Temps total : {time_MAP + time_SHUFFLE + time_REDUCE + time_SORT:.5f}')

if __name__ == '__main__':
  main("/tmp/sdelarue/machines.txt")
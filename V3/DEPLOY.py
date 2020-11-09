from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer
from multiprocessing import Pool

from ssh_connexions_utils import mkdir_cmd, sshcopy_file_cmd, getName

from termcolor import colored

def ssh(machine):
    ''' 
    Objectif
        Teste une connexion ssh sur une machine passée en paramètre.
        Si la connexion est un succès, certains traitements sont effectués sur la machine distante.
        Si le temps de connexion dépasse un timeout, le processus est killé.
    input
        machine : Nom de la machine distante'''

    cmd = "ssh -o \'StrictHostKeyChecking=no\' " + getName() + machine + ' hostname'
    timeout = 10
    # Processus de connexion ssh sur machine distante
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)

    try:
        # La sortie d'erreur est redirigée sur la sortie standard
        # Si la connexion réussi avant le timeout, on appelle la commande 'hostname' sur la machine distante
        _, stderr = process.communicate(timeout=timeout)
        
        if stderr == "":
            # Processus de création de répertoire sur la machine distante
            direct = '/tmp/sdelarue'
            mkdir_process = Popen(mkdir_cmd(machine, direct), shell=True, text=True)
            mkdir_process.wait()

            # Processus de copie de fichier sur la machine distante
            file_to_copy = 'SLAVE.py'
            sshcopy_file_process = Popen(sshcopy_file_cmd(machine, file_to_copy, 'current', direct), stdout=PIPE, stderr=PIPE, shell=True, text=True)
            sshcopy_file_process.wait()

            process.kill()
            return f"Machine : {machine:<11} | Deploiement {colored('OK', 'green')}"
        else: 
            process.kill()
            return f"Machine : {machine:<11} | Deploiement {colored('Echec', 'red')}"

    except TimeoutExpired:
        process.kill()
        return f"Machine : {machine:<11} | Deploiement {colored('TimeOut Echec', 'red')}"
        


def deploy(machine_list):
    ''' Objectif
            Lit un fichier contenant la liste des noms de machines et déploie le fichier SLAVE.py
            sur chacune d'elles '''

    print('===================================================')
    print('Deploiement du SLAVE.py sur les machines ...')

    # MultiProcessing
    with Pool() as p:
        log = p.map(ssh, machine_list)

    for elem in log:
       print(elem)

    print('---------------------------------------------------')
    print(f"Connexions réussies : {len([elem for elem in log if 'OK' in elem])}")
    print(f"Connexions en échec : {len([elem for elem in log if 'Echec' in elem])}")
    
    available_machines = [machine.split(' ')[2] for machine in log if 'OK' in machine]

    # Creation d'un fichier de machines disponibles
    with open('available_machines.txt', 'w') as f1:
        for machine in available_machines:
            f1.write(machine + '\n')
    
    return available_machines
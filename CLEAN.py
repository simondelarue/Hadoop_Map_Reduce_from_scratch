from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer
from multiprocessing import Pool

from ssh_connexions_utils import rmdir_cmd, read_machines, getName


def clean(machine):
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

            # Suppression du directory
            direct = '/tmp/sdelarue/'
            rmdir_process = Popen(rmdir_cmd(machine, direct), shell=True, text=True)
            rmdir_process.wait()

            process.kill()
            return f"Machine clean : {machine} -> OK"
        else: 
            process.kill()
            return f"Machine clean : {machine} -> Echec"

    except TimeoutExpired:
        process.kill()
        return f"Machine clean : {machine} -> Echec TimeOut"


def ssh_connect_and_rm(machine_list):
    ''' Objectif
            Lit un fichier contenant la liste des noms de machines et appelle la fonction ssh()
            sur chacune d'elles '''
    
    # MultiProcessing
    with Pool() as p:
        log = p.map(clean, machine_list)

    
    print('---------------------------------------------------')
    print(f"Connexions réussies : {len([elem for elem in log if 'OK' in elem])}")
    print(f"Connexions en échec : {len([elem for elem in log if 'Echec' in elem])}")
    print('---------------------------------------------------')
    for elem in log:
        print(elem)
    
    return [machine.split(' ')[3] for machine in log if 'OK' in machine]

machines = read_machines('/tmp/sdelarue/machines.txt')
ssh_connect_and_rm(machines)
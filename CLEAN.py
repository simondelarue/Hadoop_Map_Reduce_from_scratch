from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer


def ssh(machine):
    ''' 
    Objectif
        Teste une connexion ssh sur une machine passée en paramètre.
        Si la connexion est un succès, certains traitements sont effectués sur la machine distante.
        Si le temps de connexion dépasse un timeout, le processus est killé.
    input
        machine : Nom de la machine distante'''

    cmd = 'ssh ' + machine + ' hostname'
    timeout = 8
    # Processus de connexion ssh sur machine distante
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)

    try:
        # La sortie d'erreur est redirigée sur la sortie standard
        # Si la connexion réussi avant le timeout, on appelle la commande 'hostname' sur la machine distante
        stdout, stderr = process.communicate(timeout=timeout)
        print(str(stdout).strip() + ' : connexion OK')

        # Suppression du directory
        direct = '/tmp/sdelarue/'
        rmdir_process = Popen(rmdir_cmd(machine, direct), shell=True)
        print('Suppression du répertoire : ', direct)
        rmdir_process.wait()
        print('Repertoire ' + direct + ' supprimé')

    except TimeoutExpired:
        # Si le timeout est dépassé, kill du processus de connexion ssh
        process.kill()
        raise TimeoutExpired(process.args, timeout)


def rmdir_cmd(machine, directory):
    ''' input
            machine : Nom de la machine distante
            directory : Chemin complet du répertoire à supprimer
        output
            String : Commande shell pour la suppression du répertoire '''
    return ('ssh ' + machine + ' rm -rf ' + directory)


def ssh_connect_and_task(filename):
    ''' Objectif
            Lit un fichier contenant la liste des noms de machines et appelle la fonction ssh()
            sur chacune d'elles 
        input
            Nom du fichier contenant la liste des machines '''
        
    with open(filename, encoding='utf8') as f:
        for line in f.readlines():
            print('\nTest de connexion sur la machine : ' + line.strip())
            ssh(line.strip())
    f.close()



ssh_connect_and_task('machines.txt')
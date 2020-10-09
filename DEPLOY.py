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
        
        # Processus de création de répertoire sur la machine distante
        direct = '/tmp/sdelarue'
        mkdir_process = Popen(mkdir_cmd(machine, direct), shell=True)
        print('Creation du répertoire : ', direct)
        mkdir_process.wait()
        print('Repertoire ' + direct + ' créé')

        # Processus de copie de fichier sur la machine distante
        file_to_copy = 'SLAVE.py'
        sshcopy_file_process = Popen(sshcopy_file_cmd(machine, file_to_copy, direct), shell=True)
        print('Copy du fichier : ', file_to_copy)
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


def sshcopy_file_cmd(machine, filename, directory):
    ''' input
            machine : Nom de la machine distante
            filename : Nom du fichier à copier
            directory : Chemin complet du répertoire dans lequel copier le fichier 'filename'
        output
            String : Commande shell pour la copie d'un fichier sur machine distante via ssh '''
    return ('scp ' + filename + ' ' + machine + ':' + directory + '/' + filename)


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


'''
Etape 7 : Un programme DEPLOY : Test de connection SSH multiple
def ssh(machine):
    cmd = 'ssh ' + machine + ' hostname'
    timeout = 8
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        print(str(stdout).strip() + ' : connexion OK')
    except TimeoutExpired:
        process.kill()
        raise TimeoutExpired(process.args, timeout)
'''
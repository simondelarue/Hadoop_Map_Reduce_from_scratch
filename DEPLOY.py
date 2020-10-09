from select import select
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from threading import Timer


# teste une connexion ssh sur l'élément passé en paramètre
# Affiche le résultat de la commande 'hostname' depuis la machine distante si la connexion est réussie
def ssh(machine):
    cmd = 'ssh ' + machine + ' hostname'
    timeout = 8
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        print(str(stdout).strip() + ' : connexion OK')
        
        # Create directory
        mkdir_process = Popen(mkdir_cmd(machine, '/tmp/sdelarue'), shell=True)
        mkdir_process.wait()

        # Copy file in directory
        sshcopy_file_process = Popen(sshcopy_file_cmd(machine, 'SLAVE.py', '/tmp/sdelarue/SLAVE.py'), shell=True)
        sshcopy_file_process.wait()

    except TimeoutExpired:
        process.kill()
        raise TimeoutExpired(process.args, timeout)

# Commande de création d'un répertoire sur machine distante
def mkdir_cmd(machine, directory):
    return ('ssh ' + machine + ' mkdir -p ' + directory)

# Commande de copie SSH d'un fichier passé en paramètre, dans le répertoire passé en paramètre
def sshcopy_file_cmd(machine, filename, directory):
    return ('scp ' + filename + ' ' + machine + ':' + directory)

# Lit le fichier contenant les noms des machines sur lesquelles tester la connexion ssh
def ssh_connect_and_task(filename):
    with open(filename, encoding='utf8') as f:
        for line in f.readlines():
            print('\nTest sur la machine : ' + line.strip())
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
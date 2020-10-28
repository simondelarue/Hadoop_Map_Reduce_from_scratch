
import os

def getName():
    return('sdelarue@')

def mkdir_cmd(machine, directory):
    ''' input
            machine : Nom de la machine distante
            directory : Chemin complet du répertoire à créer 
        output
            String : Commande shell pour la création du répertoire '''
    return (f'ssh {getName()}{machine} mkdir -p {directory}/{{shufflesreceived,splits}}')


def rmdir_cmd(machine, directory):
    ''' input
            machine : Nom de la machine distante
            directory : Chemin complet du répertoire à supprimer
        output
            String : Commande shell pour la suppression du répertoire '''
    return (f'ssh {getName()}{machine} rm -rf {directory}')


def sshcopy_file_cmd(machine, filename, from_directory, to_directory):
    ''' input
            machine : Nom de la machine distante
            filename : Nom du fichier à copier
            from_directory : Chemin complet du répertoire contenant le fichier 'filename'
            to_directory : Chemin complet du répertoire dans lequel copier le fichier 'filename'
        output
            String : Commande shell pour la copie d'un fichier sur machine distante via ssh '''
    return (f'scp {from_directory}/{filename} {getName()}{machine}:{to_directory}/{filename}')


def list_files_from_dir(direct):
    ''' input
            direct : chemin du repertoire contenant les fichiers splits
        output
            liste des noms des fichiers de splits '''
    files = os.listdir(direct)
    return files


def read_machines(filename):
    ''' Lecture d'un fichier contenant le nom des machines à tester'''
    return open(filename, encoding='utf8').read().split()


def map_files_machines(file_list, machine_list):
    ''' Affecte les splits de données sur les machines '''
    return {fic : machine_list[file_list.index(fic) % len(machine_list)] for fic in file_list}


def print_data_repartition(dict_mapping):
    for split_name, machine in dict_mapping.items():
        print(f'{split_name}  -> {machine}')


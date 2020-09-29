import sys
import time

# Ouvre un fichier et compte le nombre d'occurences de chaques mots
def wc(filename):
    wc_dict = {}
    with open(filename, encoding='utf8') as f:
        for line in f.readlines():
            for word in line.split():
                # word = word.lower()
                wc_dict[word] = wc_dict.get(word, 0) + 1
    return wc_dict

# Trier un dictionnaire par clés
def print_words(dict):
    words = sorted(dict.keys())
    return sorted_wc

# Trier un dictionnaire par valeurs
def sort_values(dict):
    sorted_wc = sorted(dict.items(), key=lambda x: x[1], reverse=True)
    return sorted_wc

# Trier un dictionnaire par valeurs puis par clés
# On filtre sur les 20 premiers éléments
def sort_values_keys(dict):
    sorted_wc = sorted(dict.items(), key=lambda x: (-x[1], x[0]), reverse=False)
    return sorted_wc

# Affiche les 20 premières sorties du wordcount
def print_dict(dict):
    for elem in dict[:20]:
        print(elem[0] + ' ' + str(elem[1]))


def main():
    if len(sys.argv) != 3:
        print('usage: ./wordcount.py {--count | --sort_values | --sort_values_keys} file')
        sys.exit(1)

    option = sys.argv[1]
    filename = sys.argv[2]

    # Lecture du fichier passé en paramètre par l'utilisateur
    start_time = time.time()
    wc_dict = wc(filename)
    print("--- %s seconds for opening file and counting words ---" % (time.time() - start_time))

    # Tri des résultats de comptage d'occurences des mots
    if option == '--count':
        start_time = time.time()
        sorted_dict = print_words(wc_dict)
        print("--- %s seconds for sorting values ---" % (time.time() - start_time))
        print_dict(sorted_dict)
    elif option == '--sort_values':
        start_time = time.time()
        sorted_dict = sort_values(wc_dict)
        print("--- %s seconds for sorting values ---" % (time.time() - start_time))
        print_dict(sorted_dict)
    elif option == '--sort_values_keys':
        start_time = time.time()
        sorted_dict = sort_values_keys(wc_dict)
        print("--- %s seconds for sorting values ---" % (time.time() - start_time))
        print_dict(sorted_dict)
    else:
        print('unknown option: ' + option)
        sys.exit(1)

if __name__ == '__main__':
    main()

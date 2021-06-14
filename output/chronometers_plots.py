import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme()

file = pd.read_csv('results.txt', delimiter=';')
fig, ax = plt.subplots(1, 2, figsize=(17, 7))

# MAP SHUFFLE REDUCE
for size in file['split_size'].unique():
    tmp = file[file['split_size'] == size]
    ax[0].plot(tmp['nb_machines'], tmp['MSR_time'], label=f'Split size = {size} Mo')
ax[0].set(xlabel = 'Nombre de machines',
    ylabel = 'Temps de traitement (secondes)')
ax[0].plot(file['nb_machines'].unique(), [42.694] * len(file['nb_machines'].unique()), label='Sequentiel')
ax[0].set_title('Temps de traitement des étapes MAP-SHUFFLE-REDUCE', fontweight="bold")
ax[0].set_ylim(ymin=0)
ax[0].legend()

# MAP SHUFFLE REDUCE SORT
for size in file['split_size'].unique():
    tmp = file[file['split_size'] == size]
    ax[1].plot(tmp['nb_machines'], tmp['MSRSt_time'], label=f'Split size = {size} Mo')
ax[1].set(xlabel = 'Nombre de machines',
    ylabel = 'Temps de traitement (secondes)')
ax[1].plot(file['nb_machines'].unique(), [59.496] * len(file['nb_machines'].unique()), label='Sequentiel')
ax[1].set_title('Temps de traitement des étapes MAP-SHUFFLE-REDUCE-SORT', fontweight="bold")
ax[1].set_ylim(ymin=0)
ax[1].legend()

plt.show()
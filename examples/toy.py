# Listar *iris*
from pjdata.data_creation import read_arff

from cururu.persistence import DuplicateEntryException
from cururu.pickleserver import PickleServer

lst = PickleServer().list_by_name('iris')
for phantom in lst:
    print(phantom)

# Armazenar dataset, sem depender do pacote pjml.
from cururu.pickleserver import PickleServer

print('Storing iris...')
try:
    PickleServer().store(read_arff('iris.arff'))
    print('ok!')
except DuplicateEntryException:
    print('Duplicate! Ignored.')

lst = PickleServer().list_by_name('iris')
for phantom in lst:
    print(phantom)

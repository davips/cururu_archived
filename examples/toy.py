# Listar *iris*
import numpy as np

from cururu.persistence import DuplicateEntryException
from cururu.pickleserver import PickleServer
from pjdata.data import Data
from pjdata.data_creation import read_arff
from pjdata.dataset import Dataset

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

# Testes            ############################
dataset = Dataset('flowers', 'Beautiful description.',
                  X={'length': 'R', 'width': 'R'}, Y={'class': ['M', 'F']})
data = Data(dataset, X=np.array([[1, 2, 3, 4], [5, 6, 7, 8]]),
            Y=np.array([[1, 2, 3, 4]]))

# Teste de gravação ############################
print('Storing Data object...')
test = PickleServer()
try:
    test.store(data, fields=['X', 'Y'])
    print('ok!')
except DuplicateEntryException:
    print('Duplicate! Ignored.')

test.fetch(data, fields=['X', 'Y'])

# Teste de leitura ############################
print('Getting Data information-only objects...')
lista = test.list_by_name('flo')
print([d.dataset.name for d in lista])

print('Getting a complete Data object...')
data = test.fetch(lista[0], fields=['X', 'Y'])
print(data.X)

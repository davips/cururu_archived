# Listar *iris*

from cururu.persistence import DuplicateEntryException
from cururu.pickleserver import PickleServer
from pjdata.content.specialdata import UUIDData
from pjdata.data_creation import read_arff

lst = PickleServer().list_by_name('iris')
for phantom in lst:
    print(phantom)

# Armazenar dataset, sem depender do pacote pjml.
from cururu.pickleserver import PickleServer

print('Storing iris...')
try:
    data = read_arff('iris.arff')[1]
    PickleServer().store(data)
    print('ok!')
except DuplicateEntryException:
    print('Duplicate! Ignored.')

lst = PickleServer().list_by_name('iris')
for phantom in lst:
    print(phantom)
    print('Fetching Xd...')
    data = PickleServer().fetch(phantom)
    print(data.Xd)

# Testes            ############################
# data = Data(X=np.array([[1, 2, 3, 4], [5, 6, 7, 8]]),
#             Y=np.array([[1, 2, 3, 4]]),
#             name='flowers', desc='Beautiful description.')
# # Xd={'length': 'R', 'width': 'R'}, Yd={'class': ['M', 'F']}

# Teste de gravação ############################
print('Storing Data object...')
test = PickleServer()
try:
    test.store(data)
    print('ok!')
except DuplicateEntryException:
    print('Duplicate! Ignored.')

test.fetch(data.hollow())

# # Teste de leitura ############################
# print('Getting Data information-only objects...')
# lista = test.list_by_name('flo')
# print([d.name for d in lista])
#
# print('Getting a complete Data object...')
# data = test.fetch(lista[0])
# print(data.X)

# # Resgatar por UUID ###########################
byuuid = PickleServer().fetch(UUIDData(data.uuid))
print(byuuid)

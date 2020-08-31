# Listar *iris*
import json
from zipfile import ZipFile
from cururu.persistence import DuplicateEntryException
from cururu.pickleserver import PickleServer
from cururu.sql.mysql import MySQL
from pjdata.aux.uuid import UUID
from pjdata.content.specialdata import UUIDData
from pjdata.creation import read_arff

lst = PickleServer().list_by_name('iris')
for phantom in lst:
    print(phantom)

# Armazenar dataset, sem depender do pacote pjml.
from cururu.pickleserver import PickleServer

print('Storing iris...')
data = 0
try:
    data = read_arff('iris.arff')[1]
    PickleServer().store(data)
except DuplicateEntryException:
    print('Duplicate! Ignored.')
d = PickleServer().fetch(UUIDData(data.uuid))
print('ok!', data.id, data.history)
print('ok!', d.id, d.history)

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

print("fetch", test.fetch(data.hollow()).id)

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
print("byuuid", byuuid)

uuid = "ĹЇЖȡfĭϹƗͶэգ8Ƀű"
data = PickleServer().fetch(UUIDData(uuid))
print("dddddddddddd", data.matrices.keys())
storage = MySQL(db="paje:paje2020@143.107.183.114/paje")
# storage.store(data)
print("------------", data)
if data is None:
    raise Exception("Download failed: " + uuid + " not found!")

print("arffing...")
arff = data.arff("No name", "No description")

print("zipping...")
zipped_file = ZipFile("/tmp/lixo.zip", 'w')
print("add...")
zipped_file.writestr(uuid, arff)
zipped_file.close()

storage = PickleServer()
uuid = "ĹЇЖȡfĭϹƗͶэգ8Ƀű"
vhist = storage.visual_history(uuid, "/tmp")
print(json.dumps(vhist, indent=4))

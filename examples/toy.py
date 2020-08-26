# Listar *iris*

from zipfile import ZipFile
from cururu.persistence import DuplicateEntryException
from cururu.pickleserver import PickleServer
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
print('ok!', d.id)

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

uuid = UUID("ĹЇЖȡfĭϹƗͶэգ8Ƀű")
storage = PickleServer()
data = storage.fetch(UUIDData(uuid))
lst = []
# TODO: show uuid along with post name in the web interface
for transformer in reversed(list(data.history)[0:]):  # Discards data birth (e.g. File).
    uuid = uuid / transformer.uuid  # Revert to previous uuid.
    data = storage.fetch(UUIDData(uuid))
    dic = {"uuid": uuid, "transformation": transformer.name, "help": transformer, "exist": data is not None}
    lst.append(dic)
print(list(reversed(lst)))

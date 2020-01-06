# Listar *iris*
from cururu.pickleserver import PickleServer

lst = PickleServer().list_by_name('iris')
for phantom in lst:
    print(phantom)

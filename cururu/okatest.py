from cururu.okaserver import OkaServer
from pjdata.content.specialdata import UUIDData
from pjdata.creation import read_arff

storage = OkaServer(post=True, token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1OTg5NjEwNzcsIm5iZiI6MTU5ODk2MTA3NywianRpIjoiZDZkYzhmOWEtNzAzZS00OGZmLWI4N2YtYzlhMTY2YjAzMWIyIiwiaWRlbnRpdHkiOiJkYXZpcHMiLCJmcmVzaCI6ZmFsc2UsInR5cGUiOiJhY2Nlc3MifQ.DZ5p4pq5F8UgtGAIaznM6a1oxghw4KSHE6OfC5U4GWs")

print("Reading file...")
data = read_arff("iris.arff")[1]

print("Storing...")
storage.store(data)

print("Fetching...")
d = storage.fetch(UUIDData("ĹЇЖȡfĭϹƗͶэգ8Ƀű"))
print(d)

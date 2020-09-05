from cururu.okaserver import OkaServer
from pjdata.content.specialdata import UUIDData
from pjdata.creation import read_arff

storage = OkaServer(post=True, token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1OTkyNzA2MDIsIm5iZiI6MTU5OTI3MDYwMiwianRpIjoiMDZhNDk0YTMtODE4Mi00NzBmLTkzNGItN2UyM2IxYTgyNGE3IiwiZXhwIjoxNjAwNTY2NjAyLCJpZGVudGl0eSI6Im9rYXRlc3QiLCJmcmVzaCI6ZmFsc2UsInR5cGUiOiJhY2Nlc3MifQ.vV23vf08NTlrsNex2P3hfUY0nQsZNeKeydQEpZzoDTM")

print("Reading file...")
data = read_arff("iris.arff")[1]

print("Storing...")
storage.store(data)

print("Fetching...")
d = storage.fetch(UUIDData("ĹЇЖȡfĭϹƗͶэգ8Ƀű"))
print(d)

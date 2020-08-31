from cururu.okaserver import OkaServer
from pjdata.content.specialdata import UUIDData
from pjdata.creation import read_arff

storage = OkaServer("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1OTg5MTYzNzAsIm5iZiI6MTU5ODkxNjM3MCwianRpIjoiMjU5YmY2YTQtNjZiMy00MGJjLTllOTAtYjQ3MTRlN2NhMmM5IiwiZXhwIjoxNTk4OTU5NTcwLCJpZGVudGl0eSI6ImRhdmlwcyIsImZyZXNoIjpmYWxzZSwidHlwZSI6ImFjY2VzcyJ9.J7tKH2_z0ChIs4SICygNL4nq3-w6FI9GxR_g9aPoe6k")

print("Reading file...")
data = read_arff("iris.arff")[1]

print("Storing...")
storage.store(data)

print("Fetching...")
d = storage.fetch(UUIDData("ĹЇЖȡfĭϹƗͶэգ8Ƀű"))
print(d)

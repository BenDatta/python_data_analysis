import requests

url = "https://dog.ceo/api/breeds/list/all"

response = requests.get(url, timeout=10)
if response.status_code == 200:
    print("API is working")
else:
    print(response.status_code)

data = response.json()
print("Data is saved to json file")

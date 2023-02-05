import requests

response_get_all = requests.get('http://127.0.0.1:5000/ads')

print(response_get_all.status_code)
print(response_get_all.json())

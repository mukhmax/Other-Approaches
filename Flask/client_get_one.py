import requests

response_get_one = requests.get('http://127.0.0.1:5000/ads/2')

print(response_get_one.status_code)
print(response_get_one.json())

import requests

response_post = requests.post('http://127.0.0.1:5000/ads',
                         json={'title': 'prodam', 'text': 'loshad234'})

print(response_post.status_code)
print(response_post.json())



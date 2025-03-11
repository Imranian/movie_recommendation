import requests
import json

api_key = '43d64229413cc934538918dd45a80f87'
url = f'https://api.themoviedb.org/3/movie/popular?api_key={api_key}&language=en-US&page=1'

response = requests.get(url)
data = response.json()

for movie in data['results']:
    print(json.dumps(movie))  # Optional: send each movie to Kafka

import json
import requests
from kafka import KafkaProducer
import time
import random

# TMDB API Setup
API_KEY = '43d64229413cc934538918dd45a80f87'
BASE_URL = 'https://api.themoviedb.org/3/movie/popular'

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sent_titles = set()  # To avoid sending duplicates

while True:
    # Random page number between 1 and 10 (TMDB usually supports up to 500 pages for popular movies)
    page = random.randint(1, 10)
    url = f'{BASE_URL}?api_key={API_KEY}&language=en-US&page={page}'

    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch movies: {response.status_code}")
        time.sleep(10)
        continue

    movies = response.json().get("results", [])

    # Shuffle movies to simulate randomness
    random.shuffle(movies)

    for movie in movies:
        title = movie.get("title")
        if title not in sent_titles:
            data = {
                "id": movie.get("id"),
                "title": title,
                "overview": movie.get("overview"),
                "rating": movie.get("vote_average")
            }
            producer.send("user-events", value=data)
            print(f"Sent to Kafka: {title}")
            sent_titles.add(title)

    # Clear sent_titles periodically to allow re-sending old ones after a while
    if len(sent_titles) > 500:
        sent_titles.clear()

    time.sleep(15)  # Wait before next round

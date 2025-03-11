# autoencoder_train.py
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense

# Load movie data
df = pd.read_parquet("hdfs://localhost:9000/user/imran/movies_data")

# Fill NaNs
df['overview'] = df['overview'].fillna("")

# TF-IDF on overview
tfidf = TfidfVectorizer(max_features=300)
tfidf_matrix = tfidf.fit_transform(df['overview']).toarray()

# Normalize rating and popularity
scaler = MinMaxScaler()
numerical_features = scaler.fit_transform(df[['rating']].fillna(0))

# Combine all features
features = np.hstack((tfidf_matrix, numerical_features))

# Split data
X_train, X_test = train_test_split(features, test_size=0.2, random_state=42)

# Autoencoder Architecture
input_dim = X_train.shape[1]
encoding_dim = 64

input_layer = Input(shape=(input_dim,))
encoded = Dense(128, activation='relu')(input_layer)
encoded = Dense(encoding_dim, activation='relu')(encoded)

decoded = Dense(128, activation='relu')(encoded)
decoded = Dense(input_dim, activation='sigmoid')(decoded)

autoencoder = Model(input_layer, decoded)

# Compile
autoencoder.compile(optimizer='adam', loss='mse')

# Train
autoencoder.fit(X_train, X_train,
                epochs=20,
                batch_size=32,
                validation_data=(X_test, X_test))

# Save model and latent vectors
encoder = Model(input_layer, encoded)
movie_embeddings = encoder.predict(features)

# Save embeddings and metadata
embedding_df = pd.DataFrame(movie_embeddings)
embedding_df['movie_id'] = df['id'].values
embedding_df['title'] = df['title'].values
embedding_df.to_parquet("movie_embeddings.parquet")

from sklearn.metrics.pairwise import cosine_similarity

def recommend(clicked_title, top_n=5):
    emb_df = pd.read_parquet("movie_embeddings.parquet")
    movie_vec = emb_df[emb_df['title'] == clicked_title].iloc[:, :-2].values
    if movie_vec.shape[0] == 0:
        return []
    similarities = cosine_similarity(movie_vec, emb_df.iloc[:, :-2].values)[0]
    emb_df['similarity'] = similarities
    recommendations = emb_df.sort_values(by='similarity', ascending=False)
    return recommendations[recommendations['title'] != clicked_title][['title', 'similarity']].head(top_n).to_dict(orient='records')

import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

# Load embeddings
embedding_df = pd.read_parquet("movie_embeddings.parquet")

# Recommend similar movies
def recommend(title, top_k=5):
    matches = embedding_df[embedding_df['title'].str.lower() == title.lower()]
    
    if matches.empty:
        print(f"Title '{title}' not found. Try one of these:")
        print(embedding_df['title'].unique()[:10])  # show top 10 titles
        return
    
    movie_vec = matches.iloc[:, :-2].values
    sims = cosine_similarity(movie_vec, embedding_df.iloc[:, :-2].values)[0]
    top_indices = sims.argsort()[::-1][1:top_k+1]
    return embedding_df.iloc[top_indices][['title', 'movie_id']]
print(recommend("Squad 36"))

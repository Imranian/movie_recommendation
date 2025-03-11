import pyarrow.parquet as pq
import pyarrow.fs as fs
from flask import Flask, render_template, request
import pandas as pd
from model import recommend  # Your trained recommendation function

app = Flask(__name__)

# Create HDFS connection
hdfs = fs.HadoopFileSystem('localhost', 9000)

# Point to the HDFS parquet directory
dataset = pq.ParquetDataset('/user/imran/movies_data', filesystem=hdfs)

# Read data into Pandas DataFrame
movie_data = dataset.read().to_pandas()

@app.before_request
def reload_data():
    global movie_data
    movie_data = dataset.read().to_pandas()

@app.route('/')
def index():
    movie_list = movie_data['title'].dropna().unique().tolist()
    return render_template('index.html', movie_list=movie_list, recommended_movies=None)

@app.route('/recommend', methods=['POST'])
def get_recommendations():
    movie_id = request.form['clicked_movie']
    print("User selected:", movie_id)

    # Call your trained recommendation function
    recommended = recommend(movie_id)  # returns list of dicts: [{'title': ..., 'similarity': ...}, ...]

    # Format recommendations nicely
    formatted_recommendations = [
        {"title": rec['title'], "similarity": f"{rec['similarity']:.2f}"} for rec in recommended
    ]

    movie_list = movie_data['title'].dropna().unique().tolist()
    return render_template('index.html', movie_list=movie_list, recommended_movies=formatted_recommendations)

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)

#  @app.route('/recommend', methods=['POST'])
# def get_recommendations():

#    clicked_movie = request.json.get('clicked_movie')
#    recommended = recommend(clicked_movie)
#    return jsonify({'recommended': recommended})


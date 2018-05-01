import requests
import gzip
import shutil
import json
import csv
import os
import configparser 
from time import sleep
from concurrent.futures import ThreadPoolExecutor

config = configparser.ConfigParser()
config.read("config.ini")

API_KEY = config["KEYS"]["TMDB_API_KEY"]
POSTER_DIR_PATH = "./posters"

def download_movie_ids_file():
    res = requests.get("http://files.tmdb.org/p/exports/movie_ids_04_28_2017.json.gz")
    if res.status_code == 200:
        with open("valid_movie_ids.json.gz", "wb") as f:
            f.write(res.content)
            with gzip.open("valid_movie_ids.json.gz", "rb") as f_in:
                with open("valid_movie_ids.json", "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
    else:
        print("Error downloading valid movie file")
        
def get_movie_ids():
    movie_ids = []
    with open("valid_movie_ids.json", "r", encoding='utf-8') as f:
        for line in f:
            try:
                movie_record = json.loads(line)
                movie_ids.append(movie_record["id"])
            except ValueError:
                print("Error decoding: " + line)
    return movie_ids

def get_movie(movie_id):
    url = "https://api.themoviedb.org/3/movie/{}?api_key={}".format(movie_id, API_KEY)
    return requests.get(url).json()

def write_movies_to_csv(movies):
    with open("movies.csv", "w", newline="") as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(["id", "imdb_id", "title", "overview", "release_date", 
                             "runtime", "genres", "popularity", "poster_path", 
                             "budget", "revenue"])
        
        for movie in movies:
            csv_params = [
                movie["id"],
                movie["imdb_id"],
                movie["title"],
                movie["overview"],
                movie["release_date"],
                movie["runtime"],
                [genre["name"] for genre in movie["genres"]],
                movie["popularity"],
                movie["poster_path"],
                movie["budget"],
                movie["revenue"]
            ]
            filewriter.writerow(csv_params)   

def get_movies_from_csv():
    csv_data = []
    with open("movies.csv", "r") as f:
        reader = csv.reader(f)
        for row in reader:
            csv_data.append(row)
    return csv_data

# TODO: use configuration API to dynamically build url
def download_poster(movie):
    poster_path = movie["poster_path"]
    url = "https://image.tmdb.org/t/p/w500/{}".format(poster_path)
    
    res = requests.get(url)
    
    if res.status_code == 200:
        with open("{}/{}.jpg".format(POSTER_DIR_PATH, movie["id"]), "wb") as f:
            f.write(res.content)
    else:
        print("Error downloading poster: " + url)
    

movie_ids = get_movie_ids()[:10]

with ThreadPoolExecutor(max_workers=20) as pool:
    movies = list(pool.map(get_movie, movie_ids))
    # Rate limiting is set to 40 requests every 10 seconds
    sleep(10)
    
write_movies_to_csv(movies)
csv_movies = get_movies_from_csv()
    
if not os.path.exists(POSTER_DIR_PATH):
    os.makedirs(POSTER_DIR_PATH)

with ThreadPoolExecutor(max_workers=20) as pool:
    list(pool.map(download_poster, movies))
    # Rate limiting is set to 40 requests every 10 seconds
    sleep(10)


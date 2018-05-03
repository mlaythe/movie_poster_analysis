import requests
import gzip
import shutil
import json
import configparser 
import threading
import queue
import numpy as np
import pandas as pd
from time import sleep

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

def get_movie(q, results):
    while not q.empty():
        task = q.get()
        index = task[0]
        movie_id = task[1]
        url = "https://api.themoviedb.org/3/movie/{}?api_key={}".format(movie_id, API_KEY)
        res = requests.get(url)
        q.task_done()

        if res.status_code != 200 and res.status_code != 404:
            print("Requeuing task: " + str(movie_id))
            q.put((index, movie_id))
        else:
            print("Done fetching movie: " + str(movie_id))
            sleep(10.0)
            data = res.json()
            if data.get("status_code"):
                results[index] = {}
            else:
                results[index] = data
        
    return True
    
def write_movies_to_csv(movies):
    headers = "id,imdb_id,title,overview,release_date,runtime,genres,"
    headers += "popularity,poster_path,budget,revenue"
    formatted_movies = None
    
    for movie in movies:
        if movie:
            genres = np.array([])
            title = movie.get("title")
            overview = movie.get("overview")
            
            if movie.get("genres"):
                genres = np.array([genre["name"] for genre in movie["genres"]])
            
            if title:
                title = title.replace(",", "").encode('utf-8')
                
            if overview:
                overview = overview.replace(",", "").encode('utf-8')
                
            csv_params = np.array([[
                movie.get("id"),
                movie.get("imdb_id"),
                title,
                overview,
                movie.get("release_date"),
                movie.get("runtime"),
                genres,
                movie.get("popularity"),
                movie.get("poster_path"),
                movie.get("budget"),
                movie.get("revenue"),
            ]], dtype=object)
    
            if formatted_movies is None:
                formatted_movies = csv_params
            else:
                formatted_movies = np.concatenate((formatted_movies, csv_params), axis=0)
 
    np.savetxt("movies.csv", formatted_movies, delimiter=",", fmt='%s', header=headers)

# TODO: use configuration API to dynamically build url
def download_poster(q):
    while not q.empty():
        movie = q.get()
        poster_path = movie["poster_path"]
        url = "https://image.tmdb.org/t/p/w342/{}".format(poster_path)
        res = requests.get(url)
        q.task_done()
        
        if res.status_code != 200 and res.status_code != 404:
            print("Status code: " + res.status_code)
            print("Requeuing task: " + str(movie["id"]) + " url:" + url)
            q.put(movie)
        else:
            print("Done fetching poster: " + str(movie["id"]))
            sleep(10.0)
            with open("{}/{}.jpg".format(POSTER_DIR_PATH, movie["id"]), "wb") as f:
                f.write(res.content)
                    
    return True

movie_ids = get_movie_ids()

movies = [{} for mv_id in movie_ids]

q = queue.Queue()

for i in range(len(movie_ids)):
    #need the index and the url in each queue item.
    q.put((i, movie_ids[i]))

threads = []
for i in range(40):
    worker = threading.Thread(target=get_movie, args=(q, movies))
    threads.append(worker)
    worker.setDaemon(True)
    worker.start()
    
q.join()

for t in threads:
    t.join()

write_movies_to_csv(movies)

#data = pd.read_csv("movies.csv", sep=",", encoding = 'utf8')

#if not os.path.exists(POSTER_DIR_PATH):
#    os.makedirs(POSTER_DIR_PATH) 
#
#q = queue.Queue()
#
#for i in range(len(movies)):
#    #need the index and the url in each queue item.
#    q.put(movies[i])
#    
#threads = []
#for i in range(20):
#    worker = threading.Thread(target=download_poster, args=(q))
#    threads.append(worker)
#    worker.setDaemon(True)
#    worker.start()
#
#q.join()
#
#for t in threads:
#    t.join()

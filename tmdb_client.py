import requests
import gzip
import shutil
import json
import csv
import os
import configparser 
import threading
import queue
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
            results[index] = res.json()
        
    return True
    
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

temp_movie_ids = movie_ids[:10000]

movies = [{} for mv_id in temp_movie_ids]

q = queue.Queue()

for i in range(len(temp_movie_ids)):
    #need the index and the url in each queue item.
    q.put((i, temp_movie_ids[i]))

threads = []
for i in range(40):
    worker = threading.Thread(target=get_movie, args=(q, movies))
    threads.append(worker)
    worker.setDaemon(True)
    worker.start()
    
q.join()

for t in threads:
    t.join()

#write_movies_to_csv(movies)
#    
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

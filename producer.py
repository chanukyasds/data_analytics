from kafka import KafkaProducer
import json
from wiki_scrapper import get_movie_information
import time


def serialize_data(data):
    return json.dumps(data).encode("utf-8")


def producer_data():
    producer = KafkaProducer(bootstrap_servers=['192.168.0.122:9092'], value_serializer=serialize_data)
    data = get_movie_information()

    for each_movie in data[1:]:
        movie_info = {"movie_name": each_movie[0],
                      "release_year": each_movie[1],
                      "collection": each_movie[2]}
        # movie_info = str(each_movie[0])+","+str(each_movie[1])+","+str(each_movie[2])
        producer.send("wiki_scrapper", movie_info)
        # print(movie_info)
        time.sleep(0.5)

    print("produced all data to the kafka topic...")

    return True

producer_data()
from kafka import KafkaConsumer
import json
import uuid

def consumer_data():
    kafka_group_id = str(uuid.uuid4())
    consumer = KafkaConsumer("wiki_scrapper",
                             bootstrap_servers='192.168.0.122:9092',
                             auto_offset_reset='earliest',
                             group_id=kafka_group_id)
    #    print("each movie {}".format(json.loads(each_movie.value)))
    data = []
    for movie_info in consumer:
        if len(data) == 49:
            break
        each_movie = json.loads(movie_info.value)
        data.append((each_movie['movie_name'], each_movie['release_year'], each_movie['collection']))

    print("consumer_group_id:"+kafka_group_id)
    print("consumed raw data from kafka topic...")
    return data


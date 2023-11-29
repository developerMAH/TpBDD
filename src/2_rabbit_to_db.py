import pika
from pymongo import MongoClient
import json
import os
import time

# Assurez-vous que votre instance MongoDB est en cours d'exécution
mongo_client = MongoClient("mongodb://admin:admin@mongodb:27017/")
db = mongo_client["tpNoSQL"]
collection = db["posts"]

def safe_connect_rabbitmq():
    channel = None
    while not channel:
        try:
            connection = pika.BlockingConnection(pika.URLParameters("amqp://rabbitmq"))
            channel = connection.channel()
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    return channel

def check_duplicate(post_id):
    # Vérifier si le post avec l'ID donné existe déjà dans MongoDB
    return collection.find_one({"@Id": post_id}) is not None

def callback(ch, method, properties, body):
    data_string = body.decode("utf-8")
    post = json.loads(data_string)

    post_id = post.get("@Id")

    if not post_id:
        print("Post ID is missing. Ignoring the message.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if check_duplicate(post_id):
        print(f"Duplicate post with id {post_id}. Ignoring the message.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Insérer le post dans MongoDB
    collection.insert_one(post)

    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"Inserted post with id {post_id} into MongoDB")

def main():
    print("Starting rabbit_to_db.py")

    wait_for_mongodb()

    channel = safe_connect_rabbitmq()

    channel.queue_declare(queue='posts_to_mongodb')
    channel.basic_consume(
        queue='posts_to_mongodb',
        on_message_callback=callback
    )
    channel.start_consuming()
    print("Done")

def wait_for_mongodb():
    while True:
        try:
            mongo_client.server_info()  # Essayer de se connecter à MongoDB
            print("Connected to MongoDB")
            break
        except Exception as e:
            print(f"Connection to MongoDB failed: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()

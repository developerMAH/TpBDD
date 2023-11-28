from elasticsearch import Elasticsearch
import pika
import json
import time

# Elasticsearch configuration
es = Elasticsearch(
    ['elasticsearch'],
    http_auth=('azerty', 'azerty'),
    port=9200,
)

def safe_connect_rabbitmq():
    channel = None
    while not channel:
        try:
            connection = pika.BlockingConnection(pika.URLParameters("amqp://rabbitmq"))
            channel = connection.channel()
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    return channel

def get_document_from_elasticsearch(index_name, document_id):
    try:
        response = es.get(index=index_name, id=document_id)
        return response['_source']
    except Exception as e:
        print(f"Error retrieving document from Elasticsearch: {e}")
        return None

def callback(ch, method, properties, body):
    data_string = body.decode("utf-8")

    # Index data in Elasticsearch
    index_name = 'index'
    es.index(index=index_name, body=json.loads(data_string))

    # Example: Retrieve a document from Elasticsearch
    document_id = 'your_document_id'
    retrieved_document = get_document_from_elasticsearch(index_name, document_id)
    if retrieved_document:
        print(f"Retrieved document from Elasticsearch: {retrieved_document}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(f"Indexed message in Elasticsearch: {data_string}")

def main():
    print("Starting rabbit_to_elasticsearch.py")
    callback()
    channel = safe_connect_rabbitmq()

    channel.queue_declare(queue='posts_to_elasticsearch')
    channel.basic_consume(
        queue='posts_to_elasticsearch',
        on_message_callback=callback
    )
    channel.start_consuming()
    print("Done")

if __name__ == "__main__":
    main()

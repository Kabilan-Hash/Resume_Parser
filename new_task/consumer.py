from kafka import KafkaConsumer
import json

def consume_messages(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Starting consumer for {topic}")
    for message in consumer:
        process_message(topic, message.value)

def process_message(topic, message):
  
    if topic == 'ocr_topic':
        handle_ocr(message)
    elif topic == 'gemini_topic':
        handle_gemini(message)
    elif topic == 'postgres_topic':
        handle_postgres(message)
    elif topic == 'elasticsearch_topic':
        handle_elasticsearch(message)
    elif topic == 'neo4j_topic':
        handle_neo4j(message)

def handle_ocr(message):
    print(f"Processing OCR for {message['filename']}")


def handle_gemini(message):
    print(f"Pushing {message['filename']} to Gemini")


def handle_postgres(message):
    print(f"Storing {message['filename']} in PostgreSQL")


def handle_elasticsearch(message):
    print(f"Indexing {message['filename']} in Elasticsearch")


def handle_neo4j(message):
    print(f"Inserting {message['filename']} into Neo4j")


if __name__ == "__main__":
    topics = ['ocr_topic', 'gemini_topic', 'postgres_topic', 'elasticsearch_topic', 'neo4j_topic']
    for topic in topics:
        consume_messages(topic)

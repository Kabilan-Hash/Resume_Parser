from kafka import KafkaProducer
import json
import base64

def send_file_to_topics(file_name, file_content):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    encoded_file = base64.b64encode(file_content).decode('utf-8')

    topics = {
        'ocr_topic': {'filename': file_name, 'file_content': encoded_file},
        'gemini_topic': {'filename': file_name, 'file_content': encoded_file},
        'postgres_topic': {'filename': file_name, 'file_content': encoded_file},
        'elasticsearch_topic': {'filename': file_name, 'file_content': encoded_file},
        'neo4j_topic': {'filename': file_name, 'file_content': encoded_file}
    }


    for topic, message in topics.items():
        producer.send(topic, value=message)

    producer.flush()
    producer.close()
    print(f"Sent file {file_name} to all topics.")

if __name__ == "__main__":

    file_name = 'sample.txt'
    file_content = b'Hello, this is a test file.' 
    send_file_to_topics(file_name, file_content)

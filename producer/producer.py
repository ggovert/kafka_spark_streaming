import json
import time
import random
import os
from confluent_kafka import Producer

# Konfigurasi Confluent Kafka
conf = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'transaction-producer'
}

# Inisialisasi Producer
producer = Producer(conf)

# create delivery report
def delivery_report(err, msg):
    """ Callback untuk mengecek status pengiriman pesan """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()}: {msg.value().decode('utf-8')}")

def load_data_from_file(filename):
    """ Membaca file data.json baris demi baris """
    data_list = []
    if not os.path.exists(filename):
        print(f"Error: File {filename} tidak ditemukan!")
        return data_list

    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()

            # check line and make sure there is no comment line
            if line and not line.startswith("//"):
                try:
                    data_list.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return data_list

def run_producer():
    # load data from file
    events = load_data_from_file("data.json")

    if not events:
        print("No valid events found in the file.")
        return
    
    print(f"Found {len(events)} valid events in the file.")
    
    while True:
        for event in events:
            # convert event to json string
            event_json = json.dumps(event)
            # send message to kafka
            producer.produce(
                'transactions', 
                event_json.encode('utf-8'), 
                callback=delivery_report)
            # wait for message to be sent
            producer.poll(0)
            print(" Sleeping for 1-2 seconds")
            time.sleep(random.uniform(1, 2))

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.flush()
        print("Producer stopped.")
        
import json
import random
import time
import logging
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException

# Set up logging to monitor the activity of the script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper function to simulate user activity data
def generate_user_activity():
    """Simulates user activity data."""
    users = ['user1', 'user2', 'user3', 'user4']
    activities = ['click', 'view', 'purchase', 'login']
    
    return {
        'user_id': random.choice(users),
        'activity': random.choice(activities),
        'timestamp': time.time()
    }

# Function to process user activity and filter out unnecessary data
def process_user_activity(activity):
    """Processes the user activity data, transforming it and filtering specific events."""
    # Transform timestamp into human-readable format
    activity['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(activity['timestamp']))
    
    # Filter out "view" events (e.g., only care about click/purchase/login)
    if activity['activity'] != 'view':
        logging.info(f"Processed activity: {activity}")
        return activity
    else:
        logging.debug(f"Ignoring activity: {activity}")
        return None

# Produce user activity data to Redpanda topic
def produce_data(client, topic_name):
    """Produces simulated user activity data to the given Redpanda topic."""
    try:
        # Get the topic and prepare a producer
        topic = client.topics[topic_name.encode('utf-8')]
        producer = topic.get_sync_producer()

        while True:
            activity = generate_user_activity()
            producer.produce(json.dumps(activity).encode('utf-8'))
            logging.info(f"Produced: {activity}")
            time.sleep(1)

    except KafkaException as e:
        logging.error(f"Error producing data to Redpanda: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

# Consume and process data from Redpanda topic
def consume_and_process_data(client, source_topic_name, processed_topic_name):
    """Consumes user activity from Redpanda topic, processes it, and writes to a processed topic."""
    try:
        # Get the topics and prepare a consumer and producer
        source_topic = client.topics[source_topic_name.encode('utf-8')]
        processed_topic = client.topics[processed_topic_name.encode('utf-8')]
        consumer = source_topic.get_simple_consumer()
        producer = processed_topic.get_sync_producer()

        # Consume messages from the source topic, process them, and write to the processed topic
        batch_size = 5  # Process in batches for efficiency
        message_batch = []

        for message in consumer:
            if message is not None:
                try:
                    activity = json.loads(message.value.decode('utf-8'))
                    processed_activity = process_user_activity(activity)
                    
                    if processed_activity:
                        message_batch.append(processed_activity)
                    
                    # When batch reaches the desired size, produce it to the processed topic
                    if len(message_batch) >= batch_size:
                        for item in message_batch:
                            producer.produce(json.dumps(item).encode('utf-8'))
                        logging.info(f"Batch produced: {message_batch}")
                        message_batch.clear()

                except json.JSONDecodeError:
                    logging.error(f"Invalid JSON format in message: {message.value}")
                except Exception as e:
                    logging.error(f"Error processing message: {str(e)}")

    except KafkaException as e:
        logging.error(f"Error consuming/producing data from/to Redpanda: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

if __name__ == '__main__':
    try:
        # Connect to Redpanda
        client = KafkaClient(hosts='localhost:9092')
        logging.info("Connected to Redpanda")

        # Define topic names
        source_topic = 'user_activity'
        processed_topic = 'user_activity_processed'

        # Start producing and consuming/processing data
        # In a production environment, these would be in separate processes or microservices.
        # Produce data in a separate thread or process
        # Here, we are demonstrating running both in sequence for simplicity.

        # Start data production
        logging.info("Starting data production...")
        produce_data(client, source_topic)

        # Start data consumption and processing
        logging.info("Starting data consumption and processing...")
        consume_and_process_data(client, source_topic, processed_topic)

    except Exception as e:
        logging.error(f"Fatal error in main execution: {str(e)}")

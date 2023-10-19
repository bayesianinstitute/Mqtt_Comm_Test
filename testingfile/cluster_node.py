import paho.mqtt.client as mqtt
import random
import time

def create_cluster(broker_address, cluster_name, num_clients, inter_cluster_topic, internal_cluster_topic):
    cluster_clients = []
    for i in range(num_clients):
        client = mqtt.Client(f"{cluster_name}_Client_{i}")
        client.connect(broker_address, 1883)
        cluster_clients.append(client)
    
    for client in cluster_clients:
        client.subscribe(inter_cluster_topic, qos=0)
        client.subscribe(internal_cluster_topic, qos=0)

    return cluster_clients

def send_inter_cluster_message(sender, inter_cluster_topic, message):
    message = f"Inter-cluster message from {sender._client_id.decode('utf-8').split('_')[-1]}: {message}"
    sender.publish(inter_cluster_topic, message)

def send_internal_cluster_message(sender, cluster_clients, internal_cluster_topic, message):
    sender_id = sender._client_id.decode('utf-8')
    sender_name = sender_id.split("_")[0]
    message = f"Internal message in {sender_name} from {sender_id}: {message}"
    for client in cluster_clients:
        if client != sender:
            client.publish(internal_cluster_topic, message)

def on_message(client, userdata, message):
    client_id = client._client_id.decode('utf-8')
    cluster_id = client_id.split("_")[0]
    
    if message.topic == internal_cluster_topic:
        print(f"Received Internal message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")
        time.sleep(2)
    elif message.topic == inter_cluster_topic:
        print(f"Received Inter-cluster message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")
        time.sleep(2)


# Define the configuration for each cluster
cluster_configs = [
    {
        "broker_address": "test.mosquitto.org",
        "cluster_name": "Cluster1",
        "num_clients": 3,
        "inter_cluster_topic": "inter-cluster-topic",
        "internal_cluster_topic": "internal-cluster-topic",
    },
    {
        "broker_address": "broker.hivemq.com",
        "cluster_name": "Cluster2",
        "num_clients": 3,
        "inter_cluster_topic": "inter-cluster-topic",
        "internal_cluster_topic": "internal-cluster-topic",
    },
]

# Create clusters and clients
clusters = []
for config in cluster_configs:
    cluster = create_cluster(
        config["broker_address"],
        config["cluster_name"],
        config["num_clients"],
        config["inter_cluster_topic"],
        config["internal_cluster_topic"]
    )
    clusters.append(cluster)

# Set the callback function for all clients
for cluster in clusters:
    for client in cluster:
        client.on_message = on_message

# Start listening for messages for all clients
for cluster in clusters:
    for client in cluster:
        client.loop_start()

# Define the inter-cluster and internal cluster topics
inter_cluster_topic = "inter-cluster-topic"
internal_cluster_topic = "internal-cluster-topic"

try:
    while True:
        for cluster in clusters:
            sender = random.choice(cluster)
            inter_message = f"Hello from {sender._client_id.decode('utf-8').split('_')[-1]}"
            send_inter_cluster_message(sender,  inter_cluster_topic, inter_message)
            internal_message = f"Internal message from {sender._client_id.decode('utf-8').split('_')[-1]}"
            send_internal_cluster_message(sender, cluster, internal_cluster_topic, internal_message)
except KeyboardInterrupt:
    for cluster in clusters:
        for client in cluster:
            client.loop_stop()
            client.disconnect()

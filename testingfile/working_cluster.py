import paho.mqtt.client as mqtt
import random

# Cluster 1 Configuration
cluster1_broker_address = "test.mosquitto.org"
cluster1_clients = []

# Create MQTT clients for Cluster 1
for i in range(3):
    client = mqtt.Client(f"Cluster1_Client_{i}")
    client.connect(cluster1_broker_address, 1883)
    cluster1_clients.append(client)

# Cluster 2 Configuration
cluster2_broker_address = "broker.hivemq.com"
cluster2_clients = []

# Create MQTT clients for Cluster 2
for i in range(3):
    client = mqtt.Client(f"Cluster2_Client_{i}")
    client.connect(cluster2_broker_address, 1883)
    cluster2_clients.append(client)

# Topic for inter-cluster communication
inter_cluster_topic = "inter-cluster-topic"

# Subscribe to the topic for communication in each cluster
for client in cluster1_clients + cluster2_clients:
    client.subscribe(inter_cluster_topic, qos=0)

# Define callback function for receiving messages
def on_message(client, userdata, message):
    client_id = client._client_id.decode('utf-8')  # Decode the client ID to a string
    cluster_id = client_id.split("_")[0]  # Extract the cluster ID from the client ID

    if message.topic == internal_cluster_topic:
        print(f"Received Internal message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")
    elif message.topic == inter_cluster_topic:
        if cluster_id == "Cluster1" and client == worker_head_node_cluster1:
            print(f"Received Inter-cluster message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")
        elif cluster_id == "Cluster2" and client == worker_head_node_cluster2:
            print(f"Received Inter-cluster message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")

# Set the callback function for all clients
for client in cluster1_clients + cluster2_clients:
    client.on_message = on_message

# Start listening for messages for all clients
for client in cluster1_clients + cluster2_clients:
    client.loop_start()

# Randomly select one worker head node from each cluster
worker_head_node_cluster1 = random.choice(cluster1_clients)
worker_head_node_cluster2 = random.choice(cluster2_clients)



# Send a message from the selected worker head node in Cluster 1 to the worker head node in Cluster 2
message_from_cluster1 = f"Hello from Cluster 1, Worker Head Node {worker_head_node_cluster1._client_id.decode('utf-8').split('_')[-1]}"
worker_head_node_cluster1.publish(inter_cluster_topic, message_from_cluster1)

message_from_cluster2 = f"Hello from Cluster 2, Worker Head Node {worker_head_node_cluster2._client_id.decode('utf-8').split('_')[-1]}"
worker_head_node_cluster2.publish(inter_cluster_topic, message_from_cluster2)

# Internal cluster communication topic
internal_cluster_topic = "internal-cluster-topic"

# Subscribe to the topic for internal communication in each cluster
for client in cluster1_clients + cluster2_clients:
    client.subscribe(internal_cluster_topic, qos=0)

# Send a message within Cluster 1
for client in cluster1_clients:
    client.publish(internal_cluster_topic, f"Internal message in Cluster 1 from {client._client_id.decode('utf-8')}")

# Send a message within Cluster 2
for client in cluster2_clients:
    client.publish(internal_cluster_topic, f"Internal message in Cluster 2 from {client._client_id.decode('utf-8')}")

# Keep the script running to receive messages
try:
    while True:
        pass
except KeyboardInterrupt:
    for client in cluster1_clients + cluster2_clients:
        client.loop_stop()
        client.disconnect()
import paho.mqtt.client as mqtt
import random
import time

class MQTTCluster:
    def __init__(self, broker_address, num_clients, cluster_name):
        self.broker_address = broker_address
        self.num_clients = num_clients
        self.clients = []
        self.cluster_name = cluster_name
        self.worker_head_node = None
        self.round = 0

    def create_clients(self):
        for i in range(self.num_clients):
            client = mqtt.Client(f"{self.cluster_name}_Client_{i}")
            client.connect(self.broker_address, 1883)
            client.subscribe(inter_cluster_topic, qos=0)
            client.subscribe(internal_cluster_topic, qos=0)
            client.on_message = self.on_message
            client.loop_start()
            self.clients.append(client)

    def on_message(self, client, userdata, message):
        client_id = client._client_id.decode('utf-8')
        cluster_id = self.cluster_name

        if message.topic == internal_cluster_topic:
            # To Receive to head Only
            if client == self.worker_head_node:
                print(f"Received Internal message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")
        elif message.topic == inter_cluster_topic:
            if self.is_worker_head(client):
                print(f"Received Inter-cluster message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")

    def get_head_node(self):
        return self.worker_head_node._client_id.decode('utf-8').split('_')[-1]


    def is_worker_head(self, client):
        return client == self.worker_head_node

    def switch_worker_head_node(self):
        self.worker_head_node = random.choice(self.clients)




    def send_inter_cluster_message(self, message):
        self.worker_head_node.publish(inter_cluster_topic, message)

    def send_internal_messages(self):
        for client in self.clients:
            if client != self.worker_head_node:
                client.publish(internal_cluster_topic, f"Internal message in {self.cluster_name} from {client._client_id.decode('utf-8')}")

# Configuration
cluster1 = MQTTCluster("test.mosquitto.org", 3, "Cluster1")
cluster2 = MQTTCluster("broker.hivemq.com", 3, "Cluster2")

inter_cluster_topic = "inter-cluster-topic"
internal_cluster_topic = "internal-cluster-topic"

# Create clients for both clusters
cluster1.create_clients()
cluster2.create_clients()

try:
    while True:
        # Switch worker head node when the round is even
        if cluster1.round % 2 == 0:
            print(f"Changing worker head in cluster1 !!!!!!!!!!!!!!!")
            cluster1.switch_worker_head_node()
            print("New Head Node :",cluster1.get_head_node())
            time.sleep(2)

        if cluster2.round % 2 == 0:
            print(f"Changing worker head in cluster2 !!!!!!!!!!!!!!!")
            cluster2.switch_worker_head_node()
            print("New Head Node :",cluster2.get_head_node())
            time.sleep(2)


        # Send messages
        message_from_cluster1 = f"Hello from Cluster 1, Worker Head Node {cluster1.get_head_node()}"
        cluster1.send_inter_cluster_message(message_from_cluster1)

        message_from_cluster2 = f"Hello from Cluster 2, Worker Head Node {cluster2.get_head_node()}"
        cluster2.send_inter_cluster_message(message_from_cluster2)

        # Send internal messages
        cluster1.send_internal_messages()
        cluster2.send_internal_messages()

        cluster1.round += 1
        cluster2.round += 1

        print("Round completed : ", cluster1.round)

        if round ==10:
            print("All Round completed : " )



        time.sleep(5)  # Sleep for 5 seconds between rounds

except KeyboardInterrupt:
    for cluster in [cluster1, cluster2]:
        for client in cluster.clients:
            client.loop_stop()
            client.disconnect()

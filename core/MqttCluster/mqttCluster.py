import paho.mqtt.client as mqtt
import random
import time

class MQTTCluster:
    def __init__(self, broker_address, num_clients, cluster_name,inter_cluster_topic,internal_cluster_topic):
        self.broker_address = broker_address
        self.num_clients = num_clients
        self.clients = []
        self.cluster_name = cluster_name
        self.worker_head_node = None
        self.round = 0

        # Topic
        self.inter_cluster_topic=inter_cluster_topic
        self.internal_cluster_topic=internal_cluster_topic

    def create_clients(self):
        for i in range(self.num_clients):
            client = mqtt.Client(f"{self.cluster_name}_Client_{i}")
            client.connect(self.broker_address, 1883)
            client.subscribe(self.inter_cluster_topic, qos=0)
            client.subscribe(self.internal_cluster_topic, qos=0)
            client.on_message = self.on_message
            client.loop_start()
            self.clients.append(client)

    def on_message(self, client, userdata, message):
        client_id = client._client_id.decode('utf-8')
        cluster_id = self.cluster_name

        if message.topic == self.internal_cluster_topic:
            # To Receive to head Only
            if client == self.worker_head_node:
                print(f"Received Internal message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")
        elif message.topic == self.inter_cluster_topic:
            if self.is_worker_head(client):
                print(f"Inter-cluster message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")
                time.sleep(2)

    # Getter for Head Node in CLuster
    def get_head_node(self):
        return self.worker_head_node._client_id.decode('utf-8').split('_')[-1]


    def is_worker_head(self, client):
        return client == self.worker_head_node

    def switch_worker_head_node(self):
        self.worker_head_node = random.choice(self.clients)

    def send_inter_cluster_message(self, message):
        self.worker_head_node.publish(self.inter_cluster_topic, message)

    def send_internal_messages(self):
        for client in self.clients:
            if client != self.worker_head_node:
                client.publish(self.internal_cluster_topic, f"Internal message in {self.cluster_name} from {client._client_id.decode('utf-8')}")
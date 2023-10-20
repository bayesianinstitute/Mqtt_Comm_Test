import paho.mqtt.client as mqtt
import time
import argparse

class MQTTCluster:
    def __init__(self, broker_address, cluster_name, client_name):
        self.broker_address = broker_address
        self.cluster_name = cluster_name
        self.client_name = client_name
        self.client = None

    def create_client(self):
        self.client = mqtt.Client(self.client_name)
        self.client.connect(self.broker_address, 1883)
        self.client.on_message = self.on_message
        self.client.loop_start()

    def on_message(self, client, userdata, message):
        client_id = client._client_id.decode('utf-8')
        cluster_id = self.cluster_name

        if message.topic == inter_cluster_topic:
            print(f"Inter-cluster message in {cluster_id} from {client_id}: {message.payload.decode('utf-8')}")

    def subscribe(self, topic, qos=0):
        self.client.subscribe(topic, qos)

    def send_inter_cluster_message(self, topic, message):
        self.client.publish(topic, message)

    def run(self):
        while True:
            message = f"Hello from {self.client_name} in {self.cluster_name}"
            self.send_inter_cluster_message(inter_cluster_topic, message)
            time.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster_name", help="Name of the cluster")
    parser.add_argument("client_name", help="Client name for MQTT communication")
    args = parser.parse_args()

    # Configuration
    inter_cluster_topic = "inter-cluster-topic"
    cluster = MQTTCluster("test.mosquitto.org", args.cluster_name, args.client_name)

    # Create a client for the specified cluster
    cluster.create_client()

    # Subscribe to the inter-cluster topic
    cluster.subscribe(inter_cluster_topic, qos=0)

    try:
        cluster.run()
    except KeyboardInterrupt:
        print("Execution interrupted.")

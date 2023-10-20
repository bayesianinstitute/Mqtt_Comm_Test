import argparse
from core.MqttCluster.mqttCluster import MQTTCluster

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster_name", help="Name of the cluster")
    parser.add_argument("num_workers", help="Number of workers")
    parser.add_argument("internal_cluster_topic", help="internal Cluster topic")
    args = parser.parse_args()

    # Configuration
    inter_cluster_topic = "inter-cluster-topic"
    cluster = MQTTCluster("test.mosquitto.org", int(args.num_workers), args.cluster_name, inter_cluster_topic, args.internal_cluster_topic)

    # Create clients for both clusters
    cluster.create_clients()

    # Run the logic for cluster1
    cluster.run()



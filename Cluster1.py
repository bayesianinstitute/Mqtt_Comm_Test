import time
from core.MqttCluster.mqttCluster import MQTTCluster


# Configuration
inter_cluster_topic = "inter-cluster-topic"
internal_cluster_topic = "internal-cluster-topic"
cluster1 = MQTTCluster("broker.hivemq.com", 3, "Cluster1",inter_cluster_topic, internal_cluster_topic)



# Create clients for  clusters
cluster1.create_clients()

try:
    while True:
        # Switch worker head node when the round is even
        if cluster1.round % 2 == 0:
            print(f"Changing worker head in cluster1 !!!!!!!!!!!!!!!")
            cluster1.switch_worker_head_node()
            print("New Head Node :",cluster1.get_head_node())
            time.sleep(2)

        # Send messages
        message_from_cluster1 = f"Hello from Cluster 1, Worker Head Node {cluster1.get_head_node()}"
        cluster1.send_inter_cluster_message(message_from_cluster1)

        # Send internal messages
        cluster1.send_internal_messages()

        cluster1.round += 1

        print("Round completed : ", cluster1.round)

        # Force to quit
        if round ==10:
            print("All Round completed : " )
            for client in cluster1:
                client.loop_stop()
                client.disconnect()



        time.sleep(5)  # Sleep for 5 seconds between rounds

except KeyboardInterrupt:
    
        for client in cluster1:
            client.loop_stop()
            client.disconnect()

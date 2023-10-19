import time
from core.MqttCluster.mqttCluster import MQTTCluster



# Configuration
inter_cluster_topic = "inter-cluster-topic"
internal_cluster_topic = "internal-cluster-topic"
cluster2 = MQTTCluster("broker.hivemq.com", 3, "cluster2",inter_cluster_topic, internal_cluster_topic)



# Create clients for  clusters
cluster2.create_clients()

try:
    while True:
        # Switch worker head node when the round is even
        if cluster2.round % 2 == 0:
            print(f"Changing worker head in cluster2 !!!!!!!!!!!!!!!")
            cluster2.switch_worker_head_node()
            print("New Head Node :",cluster2.get_head_node())
            time.sleep(2)

        # Send messages
        message_from_cluster2 = f"Hello from Cluster 1, Worker Head Node {cluster2.get_head_node()}"
        cluster2.send_inter_cluster_message(message_from_cluster2)

        # Send internal messages
        cluster2.send_internal_messages()

        cluster2.round += 1

        print("Round completed : ", cluster2.round)

        if round ==10:
            print("All Round completed : " )
            for client in cluster2:
                client.loop_stop()
                client.disconnect()



        time.sleep(5)  # Sleep for 5 seconds between rounds

except KeyboardInterrupt:
    
        for client in cluster2:
            client.loop_stop()
            client.disconnect()

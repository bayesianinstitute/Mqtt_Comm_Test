# Mqtt_Comm_Test
Testing MQTT and Experiment

## Overview
This project is focused on testing MQTT communication and conducting experiments with MQTT clusters.

## Getting Started
To run the project, you can use the following steps:



### Installation
1. Clone this repository to your local machine.

```
git clone https://github.com/bayesianinstitute/Mqtt_Comm_Test.git
```

Navigate to the project directory.
```
cd Mqtt_Comm_Test
```

### Running the Project
  Use the following command to run the project:

```
python Cluster.py <cluster_name> <num_workers> <internal_cluster_topic>
<cluster_name>: Name of the cluster.
<num_workers>: Number of workers.
<internal_cluster_topic>: Internal cluster topic.
```
#### Example

```

python Cluster.py USA 3 internal-cluster-topic-USA
```

### Core Functions
In the project's core, it contains the main MqttCluster class that handles the MQTT cluster communication. The core functionalities include:

Creating and managing MQTT clients.
Running the logic for the cluster.
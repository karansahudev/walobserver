# walobserver
hbase-emr-kafka-producer

### Create a Bootstrap Script:

1. The script set-env.sh sets the environment variables and ensures they are available to all processes by appending them to /etc/profile.
Upload the Script to S3:

2. The script is uploaded to S3 to make it accessible to the EMR cluster during initialization.
Create the EMR Cluster with the Bootstrap Action:

3. The AWS CLI command creates the EMR cluster and specifies the bootstrap action to run the set-env.sh script, setting the environment variables on all nodes.

### Verifying the Environment Variables
   After the cluster is up and running, you can verify that the environment variables are set by SSHing into one of the nodes and checking the values:
   Copy code

```bash
ssh -i your-key-pair.pem hadoop@<master-node-public-dns>
source /etc/profile
echo $KAFKA_BOOTSTRAP_SERVERS
echo $KAFKA_TOPIC 
echo $SCHEMA_PATH
```
This should print the values of the environment variables you set in the set-env.sh script.

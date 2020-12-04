import time
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway
import kafka
import configparser
from kafka import TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import json
import random
import sys


## The function to parse broker, topic in and out, observations from the param file, prior parameters for MAP and params for magnitude distribution(alpha,mu) 
# @param param_file -- the name or path of the parameters file
#
# @return The kafka broker, the topic to read from (cascade_series), the topic to post messages in (cascade_poperties), alpha, mu, prior params
def get_broker_topics(param_file):
    config = configparser.ConfigParser(strict=False)
    config.read(param_file)
    broker = config['kafka']['brokers']
    topic_in = config['topic']['stats']
    observations = config['times']['observation'].split(",")
    return broker, topic_in, observations


def read_msg(msg):
    msg = msg.value.decode()
    d = json.loads(msg.replace("\'", "\""))
    return d

        
if __name__ == "__main__":
    broker, topic_in, observations = get_broker_topics(sys.argv[1])
    consumer = kafka.KafkaConsumer(**{ "bootstrap_servers":[broker],"auto_offset_reset":"earliest","group_id":"monitors"})
    consumer.subscribe(topic_in)
    registry = CollectorRegistry()
    metrics = {}
    for t in observations:
        name_metric = 'ARE_' + str(t)
        metrics[t] = Gauge(name_metric, 'Absolute relative error', registry=registry)
        metrics[t].inc()
    for msg in consumer:
        data = read_msg(msg)
        metrics[str(data["t_obs"])].set(float(data["ARE"]))
        job_name = 'batchARE'
        push_to_gateway('gateway-service:9091', job=job_name, registry=registry)
        time.sleep(1)


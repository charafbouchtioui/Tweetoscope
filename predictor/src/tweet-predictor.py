##
# @brief Defines the tweet-predictor node
#
# @file tweet-predictor.py
#
# The tweet-predictor serves to compute the estimated size of cascades using estimated size
# from the hawkes process corrected by the factor W from the learner (Random Forest), The tweet-predictor
# node also sends training samples to the learner as soon as it receives the params from the 
# estimator and size from the collector
# 
# The tweet-predictor sends the ARE to the topic stats to keep track on the performance of the predictions.
#
# @section notes_estimator Notes
# - Comments are Doxygen compatible.
#

import os
import numpy as np
import scipy.optimize as optim
import kafka
import configparser
from kafka import TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import json
import sys
#sys.path.append("../../logger")
#import logger
import pickle


## @brief The function to parse broker, topics, observations and params for magnitude distribution(alpha,mu) from the param file
#
# @param param_file -- the path of the parameters file
#
# @return The kafka broker, the topic_in(cascade_properties),topics out, alpha, mu
def get_broker_topics(param_file):
    config = configparser.ConfigParser(strict=False)
    config.read(param_file)
    broker = config['kafka']['brokers']
    topic_in = config['topic']['cascade_properties']
    topic_samples = config['topic']['samples']
    topic_models = config['topic']['models']
    topic_alerts = config['topic']['alerts']
    topic_stats = config['topic']['stats']
    alpha = float(config['parameters']['alpha'])
    mu = float(config['parameters']['mu'])
    observations = config['times']['observation'].split(',')
    return broker,topic_in,topic_models,topic_samples,topic_alerts,topic_stats,alpha,mu,observations

## @brief A function to calculate the factor (output of the RF) for a specific observation window
# 
# @param t_obs -- the observation window
# @param n_supp -- The estimated number of future retweets by the hawkes process
# @param p -- the p parameter of the hawkes process
# @param beta -- the beta parameter of the hawkes process
# @param global_model -- a map storing the most recent trained random forest model for an observation window
# @param global_alpha -- The parameter of the power law distribution  of magnitude
# @param global_mu -- The parameter mu of the power law distribution of magnitude
#
# @return The factor to correct the estimation of future retweets
def calculate_W(t_obs, n_supp, p, beta):
    global models, alpha, mu

    EM = mu * (alpha - 1) / (alpha - 2)
    n_star = p * EM
    G1 = n_supp * (1 - n_star)
    X = np.array([beta, n_star, G1]).reshape(1, -1)
    try:
        W = models[t_obs].predict(X)[0]
    except KeyError:
        #logger.info("the model for " + " T_obs= " + str(t_obs) + " is not received yet")
        W = 1
    return W

## @brief Function to calculate the tweet popularity (total number of times it got retweeted)
#
# @param n_obs -- The number of tweets observed in the cascade for a given observation window
# @param n_supp -- The estimated future retweets by hawkes process
# @param W -- The factor calculated from the random forest (default 1 if no model exists yet)
#
# @return The estimated popularity of the tweet
def calculate_n_est(n_obs, n_supp, W):
    return round(n_obs + W*n_supp)

## @brief Function to produce the message to send to the topic alerts
#
# @param t_obs -- the observation window
# @param d_params -- The message of type parameters sent by the estimator node
# @param n_est -- The estimated tweet popularity 
#
# @return empty string (serves as the key for the kafka message), the value of the message of type alert (map)
def produce_alert(t_obs, d_params, n_est):
    d = {"type":"alert", "cid":d_params["cid"], "msg":d_params["msg"], 
        "t_obs":t_obs, "n_tot":n_est}
    return d

## @brief Function to produce the message to send to the topic stats
#
# The message sent to the topic stats contains the absolute relative error 
# to track the performance of the predictions
#
# @param n_est -- The estimated tweet popularity 
# @param n_true -- The true size of the cascade extracted from the size message in topic cascade_properties
# @param t_obs -- The observation window
# @param cid -- The id of the tweet
#
# @return empty string (serves as the key for the kafka message), the value of the message of type stats (map)
def produce_stat(n_est, n_true, t_obs, cid):
    d = {"type":"stat", "cid":cid, 
        "t_obs":t_obs, "ARE":abs(n_est - n_true)/n_true}
    return d

## @brief Function to produce the message to send to the topic samples
#
# The messages sent to the topic samples serve as training data for 
# the random forest models
#
# @param t_obs -- The observation window 
# @param cid -- id of the tweet
# @param n_true -- The true size of cascade
# @param n_obs -- The number of observed tweets for a given observation time
# @param n_supp -- The estimated numbe of the future tweets
# @param p -- the p param of hawkes process
# @param beta -- the beta param of the hawkes process
# 
# @return the key and value of the message to send to topic samples
def produce_sample(t_obs, cid, n_true, n_obs, n_supp, p, beta):
    global alpha, mu
    EM = mu * (alpha - 1) / (alpha - 2)
    n_star = p * EM
    G1 = n_supp * (1 - n_star)
    X = [beta, n_star, G1]
    W = (n_true - n_obs) / n_supp
    if W < 0 :
        #logger.critical("Encountered W < 0 (n_true < n_obs)")
        #print("Encountered W < 0 (n_true < n_obs)")
        return None,None
    d = {"type":"sample", "cid":cid, "X":X, "W": W}
    return str(t_obs), d

## @brief Function to process messages received from the topic cascades_properties
#
# This Function takes a message received from cascade_properties, if the message type is parameters
# we calculate the estimated popularity of the tweet and send the message to the alert topic,
# we also check if the key (obs_window,cid) exists in the global data and add some extracted 
# informations from the params message.
# The global data saves informations from params and size messages and once for a given
# pair (obs_window,cid) we received both messages, it triggers the sending of samples to 
# the learner and stats to measure performance
# 
# @param msg -- The message received from cascade_properties
# @param topic_alerts -- The topic to send alerts to
# @param topic_stats -- The topic to send stats to
# @param topic_samples -- The topic to send samples to
# @param producer -- The kafka producer
#
def process_msg(msg,producer,topic_alerts,topic_stats,topic_samples,observations):
    global data

    t_obs = int(msg.key.decode())
    msg = msg.value.decode()
    d = json.loads(msg.replace("\'", "\""))
    if d['type'] == 'parameters':
        n_obs = d['n_obs']
        n_supp = d['n_supp']
        W = calculate_W(t_obs, n_supp, d['params'][0], d['params'][1])
        n_est = calculate_n_est(n_obs, n_supp, W)
        value = produce_alert(t_obs, d, n_est)
        producer.send(topic_alerts,value = value)
        try:
            _ = len(data[(t_obs, d['cid'])])
            data[(t_obs, d['cid'])] += [n_est, n_obs, n_supp, d['params'][0], d['params'][1]]
        except KeyError:
            data[(t_obs, d['cid'])] = [n_est, n_obs, n_supp, d['params'][0], d['params'][1]]
    
    else:
        n_true = d['n_tot']
        try:
            n = len(data[(t_obs, d['cid'])])
            data[(t_obs, d['cid'])] = [n_true] + data[(t_obs, d['cid'])]
        except KeyError:
            data[(t_obs, d['cid'])] = [n_true]
    
    if len(data[(t_obs, d['cid'])]) == 6:
        n_true = data[(t_obs, d['cid'])][0]
        n_est = data[(t_obs, d['cid'])][1]
        n_obs = data[(t_obs, d['cid'])][2]
        n_supp = data[(t_obs, d['cid'])][3]
        p = data[(t_obs, d['cid'])][4]
        beta = data[(t_obs, d['cid'])][5]
        value = produce_stat(n_est, n_true, t_obs, d['cid'])
        producer.send(topic_stats,value = value)
        key, value = produce_sample(t_obs, d['cid'], n_true, n_obs, n_supp, p, beta)
        if key!=None and value!=None:
            producer.send(topic_samples, key = str.encode(key), value = value, partition=get_partition(key,observations))

## @brief function to update the models sent by the learner
# 
# @param msg -- The message of the pickled model received from the leaner
# @param global_models -- Global variable (map) to store pairs (obs_window,model)
def update_models(msg):
    global models
    t_obs = int(msg.key.decode())
    model = pickle.loads(msg.value)
    models[t_obs] = model

## @brief Get the partition to send the message to based on the same logic messages of type size were sent by the tweet-collector
# @param key -- the observation time
# @param l -- the list of observation times sorted in increasing order
#
# @return an integer from 0 to 3 refering to the corresponding partition
def get_partition(key,l):

    ## The modulo 4 is because by default all the created topics contain 4 partitions
    return l.index(key)%4

if __name__ == "__main__" :
    ## Global variable to store informations from messages of types params and size
    data = dict()  
    ## Global variable to store the models                         
    models = dict()                        
    ## parameters extracted from the params_py.config
    broker,topic_in,topic_models,topic_samples,topic_alerts,topic_stats,alpha,mu,observations = get_broker_topics(sys.argv[1])
    #logger = logger.get_logger('predictor', broker_list=broker, debug=True)
    ## The properties of the consumer consuming from cascade_properties
    consumerProperties = { "bootstrap_servers":[broker],
                            "auto_offset_reset":"earliest", 
                            "group_id":"predictors", 
                            }
    ## The properties of the consumer consuming from models
    consumerModels = { "bootstrap_servers":[broker],
                    "auto_offset_reset":"earliest", 
                    "group_id":"models", 
                    }
    ## The kafka producer
    producer = kafka.KafkaProducer(bootstrap_servers = [broker], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer1 = kafka.KafkaConsumer(**consumerProperties)
    consumer2 = kafka.KafkaConsumer(**consumerModels)
    consumer1.subscribe(topic_in)
    consumer2.subscribe(topic_models)
    
    try:
        ## iterate over messages received from cascade_properties
        for msg in consumer1:
            ## start by checking if a new model is received
            records = consumer2.poll()
            if records is not None: 
                for topicPartition, consumerRecords in records.items():
                    for record in consumerRecords:
                        ##update the model
                        update_models(record)
            ## process the message received from cascade_properties
            process_msg(msg,producer,topic_alerts,topic_stats,topic_samples,observations)
    finally:
        ## Close consumer connection
        consumer1.close()
        consumer2.close()
    ## Flush: force purging intermediate buffers before leaving
    producer.flush()

   

    
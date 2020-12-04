##
# @brief Defines the tweet-learner node
#
# @file tweet-learner.py
#
# 
#
# @section notes_estimator Notes
# - Comments are Doxygen compatible.
#

## Imports
import numpy as np
import scipy.optimize as optim
import kafka
import configparser
import os
import sys
from kafka import TopicPartition
import json
import random
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, RandomizedSearchCV, GridSearchCV
from sklearn.metrics import mean_squared_error
#sys.path.append("../../logger/")
#import logger
import pickle
from kafka.admin import KafkaAdminClient, NewTopic

## @brief The function to parse broker, topics, observations and params for magnitude distribution(alpha,mu) from the param file
#
# @param param_file -- the path of the parameters file
#
# @return The kafka broker, the topic_in(samples),topics out(models), mini_batch, batch
def get_broker_topics(param_file):
    config = configparser.ConfigParser(strict=False)
    config.read(param_file)
    broker = config['kafka']['brokers']
    topic_in = config['topic']['samples']
    topic_models = config['topic']['models']
    mini_batch = int(config['parameters']['mini_batch'])
    batch = int(config['parameters']['batch'])
    return broker,topic_in,topic_models,mini_batch,batch

## @brief Function to read messages from the topic samples
#
# @param msg -- The msg received from the samples
# 
# @return the observation window, list of 4 elements containing 1 training sample (label w included)
def read_msg(msg):
    t_obs = int(msg.key.decode())
    msg = msg.value.decode()
    d = json.loads(msg.replace("\'", "\""))
    X, W = d['X'], d['W']
    return t_obs, X + [W]

## @brief Function to update the models based on the number of samples received
#
# This function takes receives the sample message, adds it to the dataFrame of samples,
# and update the count for the corresponding observation window. When the number of 
# samples is exactly the mini_batch or a multiple of the batch, this triggers the hyperparams 
# tunning. 
# When the number of samples for an observation window is a multiple of the mini-batch,
# this triggers only the fitting of the model with the existing hyperparams.
# The trained models are sent by the producer to the predictor, and serve to improve 
# continously the performance of the predictions.
# 
#
# @param msg -- The message received from the topic samples
# @param producer -- Kafka producer to send models
# @param topic_models -- The kafka topic (models)
# @param global_mini_batch -- int : when a mini_batch of samples is received, the model is updated (fitted)
# @param global_batch -- int : when a batch of samples is received, we trigger the hyper params tunning of the random forest model
# @param global_clfs -- Map to keep tracking models for each observation window
# @param global_data -- dataFrame to store received samples
# @param global_best_params -- Map to keep tracking the best hyperparameters found for an observation window
# @param global_count -- Map to keep tracking the number of sample received for each observation window and compare it to mini_batch and batch
def update_models(msg, producer, topic_models):
    global mini_batch, batch, data, clfs, best_params, count

    t_obs, xy = read_msg(msg)

    data = data.append(pd.DataFrame([[t_obs]+xy], columns= ['t_obs', 'beta', 'n_star', 'G1', 'W']), ignore_index=True)
    X = data[data['t_obs']==t_obs][['beta', 'n_star', 'G1']]  # Features
    y = data[data['t_obs']==t_obs]['W']  # Labels
    
    try:
        _ = count[t_obs]
    except KeyError:
        count[t_obs] = 1
    
    if count[t_obs] == mini_batch or count[t_obs] % batch == 0:
        clfs[t_obs], best_params[t_obs] =  choose_hyperparams(X, y)
        clfs[t_obs].fit(X,y)
        producer.send(topic_models, key = str(t_obs), value = clfs[t_obs])
        #logger.info("Hyperparams tunning completed and model updated for observation window " + str(t_obs))
    else:
        if count[t_obs] % mini_batch == 0:
            clfs[t_obs] =  RandomForestRegressor(**best_params[t_obs])
            clfs[t_obs].fit(X,y)
            producer.send(topic_models, key = str(t_obs), value = clfs[t_obs])
            #logger.info("Model fitted and updated for observation window"+ str(t_obs))
    count[t_obs] += 1



## @brief a function to hypertune the Random Forest models
#
# We start by using a randomized search over a large set of hyperparams, the best 
# params are choosed based on a 3-fold cross validation score. Then we use grid search 
# on tiny intervals around the params found by the randomized search.
# 
# @param X_train -- the samples [[beta,n_star,G1],....}
# @param y_train -- the labels [W,.....]
# 
# @return the best estimator fitted to our data, the best hyperparameters of that estimator
def choose_hyperparams(X_train, y_train):
    ## Create the random grid
    random_grid = {'max_depth': [x for x in range(30, 111, 20)] + [None],
                'n_estimators': [x for x in range(10, 111 , 10)]
                }
    
    rf = RandomForestRegressor()
    rf_random = RandomizedSearchCV(estimator = rf, param_distributions = random_grid, n_iter = 10, cv = 3, verbose=0, random_state=42, n_jobs = -1)
    ## Fit the random search model
    rf_random.fit(X_train, y_train)
    new_grid = rf_random.best_params_

    ## Create the parameter grid based on the results of random search 
    
    param_grid = {'max_depth': [new_grid['max_depth']],
        'n_estimators': [new_grid['n_estimators']-5, new_grid['n_estimators'], new_grid['n_estimators']+5, new_grid['n_estimators']*2]
        }
    ## Create a based model
    rf = RandomForestRegressor()
    ## Instantiate the grid search model
    grid_search = GridSearchCV(estimator = rf, param_grid = param_grid, cv = 3, n_jobs = -1, verbose = 0)
    
    ## Fit the grid search to the data
    grid_search.fit(X_train, y_train)
    return grid_search.best_estimator_, grid_search.best_params_


if __name__ == "__main__" :

    ## Create an empty dataframe to manipulate the data
    data = pd.DataFrame(columns= ['t_obs', 'beta', 'n_star', 'G1', 'W'])
    ## Create dicts to keep track of models and hyperparameters
    clfs = dict()
    best_params = dict()
    ## Count number of samples for each t_obs
    count = dict()
    ## get the parameters from the params_py.config
    broker,topic_in,topic_models,mini_batch,batch = get_broker_topics(sys.argv[1])
    #logger = logger.get_logger('learner', broker_list=broker, debug=True)

    ## Consumer properties
    consumerSamples = { "bootstrap_servers":[broker],
                        "auto_offset_reset":"earliest", 
                        "group_id":"learners", 
                        }
    ## Create the producer to post messages to models
    producer = kafka.KafkaProducer(bootstrap_servers = [broker], value_serializer=lambda v: pickle.dumps(v), key_serializer=str.encode)
    consumer = kafka.KafkaConsumer(**consumerSamples)
    consumer.subscribe(topic_in)
    for msg in consumer:
        ## Update the models and send them
        update_models(msg, producer, topic_models)
    ## Flush: force purging intermediate buffers before leaving
    producer.flush()
        



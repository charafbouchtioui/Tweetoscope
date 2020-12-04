##
# @brief Defines the tweet-estimator node
#
# @file tweet-estimator.py
#
# @brief Defines how to estimate cascades lenght using Hawkes processes.
#
# @section notes_estimator Notes
# - Comments are Doxygen compatible.
#


##Imports
import numpy as np
import scipy.optimize as optim
import kafka 
import configparser
import os
from kafka import TopicPartition
import sys
import json
#sys.path.append("../../logger/")
#import logger



## The function to parse broker, topic in and out, observations from the param file, prior parameters for MAP and params for magnitude distribution(alpha,mu) 
# @param param_file -- the name or path of the parameters file
#
# @return The kafka broker, the topic to read from (cascade_series), the topic to post messages in (cascade_poperties), alpha, mu, prior params
def get_broker_topics(param_file):
    config = configparser.ConfigParser(strict=False)
    config.read(param_file)
    broker = config['kafka']['brokers']
    topic_in = config['topic']['cascade_series']
    topic_out = config['topic']['cascade_properties']
    observations = config['times']['observation'].split(",")
    alpha = float(config['parameters']['alpha'])
    mu = float(config['parameters']['mu'])
    prior_params = config['parameters']['prior_params'].split(",")
    prior_params = [float(x) for x in prior_params]


    return broker,topic_in,topic_out,observations,alpha,mu,prior_params

## @brief Calculates the supplementary expected number of retweets in a cascade
#
#@param params   -- parameter tuple (p,beta) of the Hawkes process
#@param history  -- (n,2) numpy array containing marked time points (t_i,m_i)  
#@param alpha    -- power parameter of the power-law mark distribution
#@param mu       -- min value parameter of the power-law mark distribution
#@param t        -- current time (i.e end of observation window)
#
#@return the supplementary expected number of retweets in a cascade
def prediction(params, history, alpha, mu, t):

    p,beta = params
    
    tis = history[:,0]
   
    EM = mu * (alpha - 1) / (alpha - 2)
    n_star = p * EM
    if n_star >= 1:
        #logger.critical("Encountered a Branching factor greater than 1 while predecting N_supp of cascade")
        raise Exception(f"Branching factor {n_star:.2f} greater than one")
    I = history[:,0] < t
    tis = history[I,0]
    mis = history[I,1]
    G1 = p * np.sum(mis * np.exp(-beta * (t - tis)))
    ##The supplementary number of tweets is returned withotu taking into account the tweets that already were in the cascade
    N_sup = G1 / (1. - n_star)
    return N_sup


## @brief Calculates the loglikelihood of a Hawkes process with exponential kernel computed with a linear time complexity
#    
# @param params   -- parameter tuple (p,beta) of the Hawkes process
# @param history  -- (n,2) numpy array containing marked time points (t_i,m_i)  
# @param t        -- current time (i.e end of observation window)
#
#@return The loglikelihood of a Hawkes process with exponential kernel 
def loglikelihood(params, history, t):
    p,beta = params    
    if p <= 0 or p >= 1 or beta <= 0.: return -np.inf

    n = len(history)
    tis = history[:,0]
    mis = history[:,1]
    
    LL = (n-1) * np.log(p * beta)
    logA = -np.inf
    prev_ti, prev_mi = history[0]
    
    i = 0
    for ti,mi in history[1:]:
        #if(prev_mi + np.exp(logA) <= 0):
            ##Send a message to the logger in case a bad values is encountered
            #logger.warning("Bad value of (magnitude + A) encountered for logliklihood calculation, the magnitude of the tweet may be 0")
            #print("Bad value of (magnitude + A) encountered for logliklihood calculation, the magnitude of the tweet may be 0")            
        ##Instead of adding log(prev_mi + A) we add log (prev_mi + A + 1) since in most cases m >> 1 and it prevents getting LL = - inf in case of a magnitude = 0 
        logA = np.log(1+ prev_mi + np.exp(logA)) - beta * (ti - prev_ti)
        LL += logA
        prev_ti,prev_mi = ti,mi
        i += 1
        
    logA = np.log(1+ prev_mi + np.exp(logA)) - beta * (t - prev_ti)
    LL -= p * (np.sum(mis) - np.exp(logA))

    return LL

## @brief Calculates the pair of the estimated logdensity of a posteriori and parameters (as a numpy array)
#
# @param history      -- (n,2) numpy array containing marked time points (t_i,m_i) 
# @param t            -- current time (i.e end of observation window)
# @param alpha        -- power parameter of the power-law mark distribution
# @param mu           -- min value parameter of the power-law mark distribution 
# @param prior_params -- list (mu_p, mu_beta, sig_p, sig_beta, corr) of hyper parameters of the prior
#            -- where:
#            --   mu_p:     is the prior mean value of p
#            --   mu_beta:  is the prior mean value of beta
#            --   sig_p:    is the prior standard deviation of p
#            --   sig_beta: is the prior standard deviation of beta
#            --   corr:     is the correlation coefficient between p and beta
# @param display      -- verbose flag to display optimization iterations (see 'disp' options of optim.optimize)
# @param max_n_star   -- maximum authorized value of the branching factor (defines the upper bound of p)
# @param display      -- verbose flag to display optimization iterations (see 'disp' options of optim.optimize)
#
# @return the loglikelihood and the parameters p and Beta of the Hawkes Process with exponential Kernel
def compute_MAP(history, t, alpha, mu,max_n_star = 1, display=False):
   
    ## prior_params is a list (mu_p, mu_beta, sig_p, sig_beta, corr) of hyper parameters of the prior
    global prior_params 
    ## Compute prior moments
    mu_p, mu_beta, sig_p, sig_beta, corr = prior_params
    sample_mean = np.array([mu_p, mu_beta])
    cov_p_beta = corr * sig_p * sig_beta
    Q = np.array([[sig_p ** 2, cov_p_beta], [cov_p_beta, sig_beta **2]])
    
    ## Apply method of moments
    cov_prior = np.log(Q / sample_mean.reshape((-1,1)) / sample_mean.reshape((1,-1)) + 1)
    mean_prior = np.log(sample_mean) - np.diag(cov_prior) / 2.

    ## Compute the covariance inverse (precision matrix) once for all
    inv_cov_prior = np.asmatrix(cov_prior).I

    ## Define the target function to minimize as minus the log of the a posteriori density    
    def target(params):
        log_params = np.log(params)
        
        if np.any(np.isnan(log_params)):
            return np.inf
        else:
            dparams = np.asmatrix(log_params - mean_prior)
            prior_term = float(- 1/2 * dparams * inv_cov_prior * dparams.T)
            logLL = loglikelihood(params, history, t)
            return - (prior_term + logLL)
      
    EM = mu * (alpha - 1) / (alpha - 2)
    eps = 1.E-8

    ## Set realistic bounds on p and beta
    p_min, p_max       = eps, max_n_star/EM - eps
    beta_min, beta_max = 1/(3600. * 24 * 10), 1/(60. * 1)
    
    ## Define the bounds on p (first column) and beta (second column)
    bounds = optim.Bounds(
        np.array([p_min, beta_min]),
        np.array([p_max, beta_max])
    )
    
    ## Run the optimization
    res = optim.minimize(
        target, sample_mean,
        method='Powell',
        bounds=bounds,
        options={'xtol': 1e-8, 'disp': display}
    )
    ## Returns the loglikelihood and found parameters
    return(-res.fun, res.x)


## @brief Function to compute params of Hawkes from an estimator and predict the supplementary number of retweets of the cascade
# @param estimator The method used to estimate hawkes params (MAP)
# @param history -- (n,2) numpy array containing marked time points (t_i,m_i)
# @param alpha  -- power parameter of the power-law mark distribution of magnitudes
# @param mu    -- min value parameter of the power-law mark distribution of magnitudes
# @param t            -- current time (i.e end of observation window)
# @param n_tries  -- number of iterations to make to calculate the predictions
#
# @return best found loglikelihood, best estimated hawkes parameters, best estimated number of future retweets
def prediction_from_estimator(estimator,history, alpha, mu, t, n_tries=1):
    
    best_LL, best_params, best_N_sup = -np.inf,[], np.inf
    tis = history[:,0]
    partial_history = history[tis <= t]
    #if len(partial_history) < len(history):
        #logger.critical("Partial cascade latest received tweet timestamp exceeded the observation window")
        #print("Partial cascade latest received tweet timestamp exceeded the observation window")
    for j in range(n_tries):
        try:
            LL, param = estimator(partial_history, t, alpha, mu)
            if LL > best_LL:
                N_sup = prediction(param, partial_history, alpha, mu, t)
                best_LL, best_params, best_N_sup = LL, param, N_sup
        except:
            pass
    
    return best_LL, best_params, best_N_sup


## @brief Function to produce the message of type parameters to send to topic cascade_parameters from the message received from cascade_series
# @param message -- message received from the cascade_series { "type" : "serie", "cid": "tw23981", "msg" : "blah blah", "T_obs": 600, "tweets": [ (t1, m1), (t2,m2), ... ] }
#
# @return key = observation window, value = map in the form of { 'type': 'parameters', 'cid': 'tw23981', 'msg' : 'blah blah', 'n_obs': 32, 'n_supp' : 120, 'params': [ 0.0423, 124.312 ] }
def produce_msg(message):
    
    ## Decode message received and store it in a map
    d = eval(message.value.decode())
    ## Store the key 
    T_obs = d["T_obs"]
    history = np.array(d["tweets"])
    ## Bring origin of times to 0
    history[:,0] = history[:,0] - history[0][0]
    cid = d["cid"]
    msg = d['msg']
    ## Calculate the number of observed tweets in the cascade
    n_obs = len(history[history[:,0]<T_obs])
    ## Calculate the parameters and the prediction of number of retweets
    LL,params, n_supp = prediction_from_estimator(compute_MAP,history, alpha, mu, T_obs, n_tries=2)
    T_obs = str(T_obs)
    if len(params)==2:
        params = params.tolist()
    else : 
        ##When the computation of the params fails, send an error to the logger
        #logger.warning("MAP estimator failed to compute the parameters for tweet " + str(cid))
        return None,None
    d_result = {"type":"parameters","cid":cid,"msg":msg,"n_obs":n_obs,"n_supp":n_supp,"params":params}
    return T_obs,d_result


## @brief Get the partition to send the message to based on the same logic messages of type size were sent by the tweet-collector
# @param key -- the observation time
# @param l -- the list of observation times sorted in increasing order
#
# @return an integer from 0 to 3 refering to the corresponding partition
def get_partition(key,l):

    ## The modulo 4 is because by default all the created topics contain 4 partitions
    return l.index(key)%4

if __name__ == "__main__" :
    
    ##Read data from the param file
    broker,topic_in,topic_out,observations,alpha,mu,prior_params = get_broker_topics(sys.argv[1])
    #logger = logger.get_logger('estimator', broker_list=broker, debug=True)
    ##To consume latest messages and commit, auto_offset automatically
    consumerProperties = { "bootstrap_servers":[broker],"auto_offset_reset":"earliest","group_id":"estimators" }
    consumer = kafka.KafkaConsumer(**consumerProperties)
    consumer.subscribe(topic_in)

    
    ## Kafka producer to send messages of type parameters to the topic out
    producer = kafka.KafkaProducer(bootstrap_servers = [broker], value_serializer=lambda v: json.dumps(v).encode('utf-8'), key_serializer=str.encode)
    for message in consumer:
        key,value = produce_msg(message)
        ## Test if the computation of key and value was successful
        if key!=None and value!=None:
            producer.send(topic_out, key = key, value = value,partition=get_partition(key,observations))
    ## Flush: force purging intermediate buffers before leaving
    producer.flush()


        
        
        


            
    
      

    

    
    



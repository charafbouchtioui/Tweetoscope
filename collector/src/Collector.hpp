/**
 * @file Collector.hpp
 * @brief The implementation of the library to collect tweets ann send partial and terminated cascades
 * @version 1.0
 * @date 2020-11-25
 * 
 * @copyright Copyright (c) 2020
 * 
 */



#pragma once
#include <iostream>
#include <iomanip>
#include <fstream>
#include <queue>
#include <thread>
#include <atomic>
#include <sstream>
#include <stdexcept>
#include <tweetoscopeCollectorParams.hpp>
#include <iterator>
#include <cppkafka/cppkafka.h>
#include <boost/heap/binomial_heap.hpp>
#include <map>
#include <utility>
#include <string>
#include <vector>
#include <tuple>


namespace tweetoscope {

  struct Cascade;
  struct Processor;
  struct Processors;

  using timestamp = std::size_t;
  
  namespace source {
    using idf = std::size_t;
  }
  namespace cascade {
    using ref   = std::shared_ptr<Cascade>;
    using ref_w = std::weak_ptr<Cascade>; 
    using idf = std::size_t; 
  }

  /**
   * @brief Class For storing tweets
   * Storing tweets received from kafka topic tweets
   */
  struct tweet {
    std::string type = ""; /**< The tweet type (tweet or retweet) */
    std::string msg  = ""; /**< The tweet message */
    timestamp time   = 0;  /**< The timestamp of the tweet relative to the tweet source*/
    double magnitude = 0;  /**< The magnitude (number of followers) of the tweet*/
    source::idf source = 0; /**< The source which sent the tweets (serves as time reference) */
    std::string info = "";  
    cascade::idf key;  /**< The cascade id  of the tweet*/
  };

  /**
   * @brief Function to return strings betwen quotation marks ("") from an istream object
   * 
   * @param is the istream object
   * @return string 
   */
  inline std::string get_string_val(std::istream& is) {
    char c;
    is >> c; /**< eats " */
    std::string value;
    std::getline(is, value, '"'); /**< eats tweet", but value has tweet */
    return value;
  }
  /**
   * @brief Construct a tweet from an istream object
   * a tweet is constructed from {"type" : "tweet"|"retweet","msg": "...","time": timestamp,
   * "magnitude": 1085.0,"source": 0, "info": "blabla"}
   * @param is istream object
   * @param t tweet
   * @return std::istream& 
   */
  inline std::istream& operator>>(std::istream& is, tweet& t) {

    std::string buf;
    char c;
    is >> c; 
    is >> c; 
    while(c != '}') {
      std::string tag;
      std::getline(is, tag, '"'); /**< Eats until next ", that is eaten but not stored into tag */
      is >> c;  /** < eats ":" */
      if     (tag == "type")    t.type = get_string_val(is);
      else if(tag == "msg")     t.msg  = get_string_val(is);
      else if(tag == "info")    t.info = get_string_val(is);
      else if(tag == "t")       is >> t.time;
      else if(tag == "m")       is >> t.magnitude;
      else if(tag == "source")  is >> t.source;

      is >> c; /**< eats either } or , */
      if(c == ',')
        is >> c; /** eats '"' */
    }
    return is;
  }
  
  /**
   * @brief The comparator used for the binomial heap priority queue
   * 
   */
  struct cascade_ref_comparator {
    /**
     * @brief Operator() to compare two cascades references 
     * 
     * @param op1 First cascade reference
     * @param op2 Second cascade reference
     * @return bool 
     */
    bool operator()(cascade::ref op1, cascade::ref op2) const; 
  };
  
  using priority_queue = boost::heap::binomial_heap<cascade::ref,
                            boost::heap::compare<cascade_ref_comparator>>; /**< Priority queue to keep track on cascades termination */
  
  /**
   * @brief Class to manage cascades
   * A cascade is composed of a tweet and the serie of its retweets
   */
  struct Cascade {
    cascade::idf key;      /**<the id of the original tweet */
    source::idf source_id;  /**< the id if the source of the tweet */
    std::string msg;        /** msg of the tweet */
    timestamp latest_t;    /**< the time of the newest retweet */
    timestamp first_t;     /**< the time of the first tweet */
    std::vector<std::pair<timestamp,double>> tweets;  /**< Vector containing times and magnitudes of the tweet and retweets */
    priority_queue::handle_type location; /**< determines the cascade location in the priority queue */

    /**
     * @brief overloading the operator < to make cascades comparaison
     * 
     * @param other The second cascade
     * @return bool 
     */
    bool operator<(const Cascade& other) const;

    /**
     * @brief Default Constructor of Cascade object
     * 
     */
    Cascade() = default;
    /**
     * @brief Construct a new Cascade object from a tweet
     * A cascade can be fully constructed from the tweet since the tweet stores 
     * also the id of the cascade. Key , msg, source, the latest time and first time are
     * all initialized from the tweet, and the time and magnitude pair is pushed to
     * the tweets vector.
     * @param t tweet
     */
    Cascade(const tweet& t) : key(t.key),source_id(t.source),msg(t.msg),latest_t(t.time),
    first_t(t.time){
      tweets.push_back({t.time,t.magnitude});
    };

    /**
     * @brief Destroy the Cascade object
     * 
     */
    virtual ~Cascade() {};
    
    /**
     * @brief overloading the operator << to stream cascade objects 
     * 
     * @param os 
     * @param c Cascade
     * @return the ostream object
     */
    friend std::ostream& operator<<(std::ostream& os, const Cascade& c);
  };

  /**
   * @brief Constructs cascades references (shared pointer)
   * A convenient way to create a cascade object 
   * @param t The first tweet to initialize the cascade object
   * @return cascade::ref 
   */
  cascade::ref cascade_ptr(const tweet& t);

  std::ostream& operator<<(std::ostream& os, const Cascade& c);

  /**
   * @brief Creates message to send partial cascades
   * Creating the message to send to kafka topic cascade_series in the format
   * Key = None Value = { "type" : "serie", "cid": "tw23981", "msg" : 
   * "blah blah", "T_obs": 600, "tweets": [ (t1, m1), (t2,m2), ... ] }
   * @param c The partial cascade
   * @param obs The observation window of the cascade
   * @return std::string -> message to send
   */
  std::string format_cascade_series(const Cascade& c, timestamp obs); 
  /**
   * @brief Creates message to send terminated cascades
   * Form of the message sent to the kafka topic cascade_properties :
   * { "type" : "size", "cid": "tw23981", "n_tot": 127, "t_end": 4329 }
   * @param c The terminated cascade
   * @return std::string -> message to send
   */
  std::string format_cascade_properties(const Cascade& c); 
  /**
   * @brief Determines which partition the message of type size is sent to
   * Cascade_properties receives messages of type size from the collector and of
   * type parameters from the estimator. The messages with the same keys need to be stored 
   * in the same partition. cascade_properties have 4 partitions, the partition is determined 
   * as the index of the observation window in the vector of all observations modulo 4.
   * @param pr Map of processors
   * @param obs The observation window
   * @return int The partition number
   */
  int get_partition(Processors& pr, timestamp obs);

  /**
   * @brief Sends kafka messages
   * If Terminated is True, a message of type size is constructed by format_cascade_properties and
   * sent to the defined partition. When Terminated is false, a message of type series is 
   * constructed and sent to cascade_series
   * @param c_ptr cascade reference
   * @param pr Map of processors
   * @param obs observation window
   * @param Terminated 
   */
  void send_kafka(cascade::ref c_ptr,Processors& pr, timestamp obs, bool Terminated);
  
  /**
   * @brief A processor manages cascades of the same source
   * 
   */
  struct Processor {
    params::collector parameters; /**< Parsed parameters from the config file*/
    priority_queue cascade_queue; /** Priority_queue to manage cascade termination */
    std::map<timestamp, std::queue<cascade::ref_w>> partial_cascade; /**< Map for managing partial cascades */
    std::map<cascade::idf, cascade::ref_w> symbols; /**< Map to facilitate finding cascade reference for its id */

    /**
     * @brief Default constructor of Processor object
     * 
     */
    Processor() = default;

    /**
     * @brief Constructs processor given a tweet and params collector
     * This constructor is used when a new source of tweets is identified, first the 
     * new cascade is constructed and the reference is pushed into the priority queue,
     * then the id and weak reference of the cascade are pushed to the symbol table,
     * and finally the weak reference of the cascade is pushed to each queue of the 
     * partial cascades map
     * @param t tweet
     * @param p parameters collector
     */
    Processor(const tweet& t, params::collector p);

    /**
     * @brief Destroy the Processor object
     * 
     */
    virtual ~Processor() {};

  };

  /**
   * @brief Class to manage processor 
   * 
   * This manages a processor map, when a tweet is received it is added to the corresponding
   * processor and cascade of the processors map. The event of receiving a tweet may 
   * trigger sending a terminated or partial cascade. 
   * 
   * The overloaded operator += of the Processors class handles adding tweets and 
   * checks for sending cascades to kafka topics
   * 
   */
  struct Processors {
    
    std::map<source::idf, Processor> processors; /** < A map to store all processors*/
    params::collector parameters; /**< Parsed parameters */
    cppkafka::Configuration config; /**< Configuration of the producer to send messages */
    cppkafka::Producer  producer; /**< Kafka Producer */

    /**
     * @brief Construct a new Processors object
     * 
     */
    Processors() = default; 
    /**
     * @brief Construct Processors from parameters file
     * This initializes the parameters member, the producer configuration
     * and the producer itself
     * @param config_filename 
     */
    Processors(const std::string& config_filename) : parameters(config_filename),
    config({{"metadata.broker.list", parameters.kafka.brokers},
      {"log.connection.close", false }}),producer(config) {};
    
    /**
     * @brief Construct Processors object from a parameters collector object
     * This constuctor is mainly used for unit tests
     * @param p 
     */
    Processors(const params::collector& p) : parameters(p),config({{"metadata.broker.list", parameters.kafka.brokers},
      {"log.connection.close", false }}),producer(config) {};
    /**
     * @brief Destroy the Processors object
     * 
     */
    virtual ~Processors() {};
    /**
     * @brief The += overloaded operator manages tweets recption and sending kafka messages
     * 
     * Case 1 : The tweet's source is new -> A processor is created and added 
     * to the processes map 
     * 
     * Case 2 : tweet received from an existing source -> Create a reference to the cascade
     * and Check if the cascade is already in the symbol table 
     *    
     *    - Step 1 Partial cascades : Using the difference timestamp of the recently 
     *                                received tweet and the first time of the corresponding 
     *                                cascade, send all partial cascades for which the difference exceeds
     *                                the observation window, and the number of tweets in the cascade
     *                                exceeds the minimum min_cascade_num
     * 
     *    If the cascade is not in the symbol table (newly created), it is pushed to the table
     *    - Step 2 Priority Queue : Using the difference between the timestamp of the received 
     *                              tweet and the latest timestamp of the cascades in the priority
     *                              queue, send all terminated cascades for which the difference
     *                              exceeds the time for considering a cascade terminated and the number 
     *                              of tweets exceeds the min_cascade_num.
     *    If the cascade is newly created, it is pushed to the queue
     *    - Step 3 update priority Queue : If the weak pointer that points to the cascade is 
     *                                     still pointing to a valid cascade object (cascade not terminated)
     *                                     and the cascade, and if the cascade is not newly created, the tweet
     *                                     is pushed the the cascade, and the priority queue is udated.
     *
     * @param t Received tweet
     */
    void operator+=(const tweet& t); 

    
  }; 
}

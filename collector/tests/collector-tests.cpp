#define BOOST_TEST_MODULE Collector test
#include <boost/test/unit_test.hpp>
#include <Collector.hpp>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <queue>
#include <thread>
#include <atomic>
#include <sstream>
#include <stdexcept>
#include <iterator>
#include <cppkafka/cppkafka.h>
#include <boost/heap/binomial_heap.hpp>
#include <map>
#include <utility>
#include <string>
#include <vector>
#include <tuple>
#include <math.h>


//Function for building tweets to use them for tests
inline tweetoscope::tweet tweet_builder(std::string type, tweetoscope::timestamp time,
                    double magnitude,tweetoscope::source::idf source,
                    tweetoscope::cascade::idf cid){
                        tweetoscope::tweet twt;
                        twt.type = type;
                        twt.time = time;
                        twt.magnitude = magnitude;
                        twt.source = source;
                        twt.msg = "bla bla";
                        twt.key = cid;
                        return twt;
                    }

inline tweetoscope::params::collector generate_params(){
    tweetoscope::params::collector parameters;
    tweetoscope::params::section::Kafka broker;
    broker.brokers = "localhost:9092";
    tweetoscope::params::section::Topic topics;
    topics.in,topics.out_properties,topics.out_series = "tweets","cascade_properties","cascade_series";
    tweetoscope::params::section::Times times;
    times.observation = {600,1200};
    times.terminated = 1800;
    tweetoscope::params::section::Cascade c;
    c.min_cascade_size = 5;
    parameters.kafka = broker;
    parameters.topic = topics;
    parameters.times = times;
    parameters.cascade = c;
    return parameters;
}

BOOST_AUTO_TEST_SUITE( tweets_and_cascades )
//Unit test to test the operator >> for creating tweets
BOOST_AUTO_TEST_CASE( Read_tweet_test ) {
    std::string s = "{\"type\": \"tweet\", \"msg\": \"bla bla\",\"t\": 10, \"m\": 60,\"info\": \"\",\"source\": 0}";
    tweetoscope::tweet twt;
    tweetoscope::cascade::idf key='tw1';
    auto istr = std::istringstream(s);
    tweetoscope::operator>>(istr,twt);
    twt.key = key;
    tweetoscope::tweet twt1 = tweet_builder("tweet",10,60,0,'tw1');
    BOOST_CHECK(twt1.key==twt.key);
    BOOST_CHECK(twt1.source==twt.source);
    BOOST_CHECK(twt1.time==twt.time);
    BOOST_CHECK(twt1.magnitude==twt.magnitude);
    BOOST_CHECK(twt1.type==twt.type);
}

//Unit test to test the construction of cascades from tweets
BOOST_AUTO_TEST_CASE( cascade_constructor_ptr ){
    tweetoscope::tweet twt1 = tweet_builder("tweet",10,60,0,'tw1');
    tweetoscope::cascade::ref c_ptr = tweetoscope::cascade_ptr(twt1);
    BOOST_CHECK((*c_ptr).tweets.size()==1);
    BOOST_CHECK( (c_ptr->tweets.front().first==twt1.time) && (c_ptr->tweets.front().second==twt1.magnitude));
}

//Unit test to test the comparaison of two cascades, used later in priority queue
BOOST_AUTO_TEST_CASE ( cascades_comparator ) {
    tweetoscope::tweet twt1 = tweet_builder("tweet",10,60,0,'tw1');
    tweetoscope::tweet twt2 = tweet_builder("tweet",15,70,0,'tw2');
    tweetoscope::cascade::ref c_ptr_1 = tweetoscope::cascade_ptr(twt1);
    tweetoscope::cascade::ref c_ptr_2 = tweetoscope::cascade_ptr(twt2);
    tweetoscope::cascade_ref_comparator comparator;
    BOOST_CHECK(comparator(c_ptr_2,c_ptr_1));
}
BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE( Processors )

BOOST_AUTO_TEST_CASE( new_source_of_tweets ) {
       tweetoscope::Processors m_p(generate_params());
       tweetoscope::tweet twt1 = tweet_builder("tweet",0,60,0,'tw1');
       m_p += twt1;
       auto it = m_p.processors.find(twt1.source);
       BOOST_CHECK(it!=m_p.processors.end());
       BOOST_CHECK(it->second.cascade_queue.size()==1);
       auto it_s = it->second.symbols.find(twt1.key);
       BOOST_CHECK(it_s != it->second.symbols.end());
       for (const auto& x:it->second.partial_cascade) {
           BOOST_CHECK(x.second.size()==1);
       }
}
BOOST_AUTO_TEST_CASE( more_than_one_source ) {
       tweetoscope::Processors m_p(generate_params());
       tweetoscope::tweet twt1 = tweet_builder("tweet",0,60,0,'tw1');
       tweetoscope::tweet twt2 = tweet_builder("tweet",0,60,1,'tw2');
       m_p += twt1;
       m_p += twt2;
       BOOST_CHECK(m_p.processors.size()==2);
}

BOOST_AUTO_TEST_CASE( order_in_priority_queue ) {
        tweetoscope::Processors m_p_1(generate_params());
        tweetoscope::tweet twt1 = tweet_builder("tweet",0,60,0,'tw1');
        tweetoscope::tweet twt2 = tweet_builder("retweet",10,60,0,'tw1');
        tweetoscope::tweet twt3 = tweet_builder("tweet",20,60,0,'tw2');
        tweetoscope::tweet twt4 = tweet_builder("retweet",40,60,0,'tw2');
        tweetoscope::tweet twt5 = tweet_builder("tweet",50,60,0,'tw3');
        tweetoscope::tweet twt6 = tweet_builder("retweet",70,60,0,'tw3');
        m_p_1 += twt1;
        m_p_1 += twt2;
        m_p_1 += twt3;
        m_p_1 += twt4;
        m_p_1 += twt5;
        m_p_1 += twt6;
        auto it = m_p_1.processors.find(twt1.source);
        BOOST_CHECK(it->second.cascade_queue.top()->latest_t == 10);
        m_p_1 += tweet_builder("retweet",80,60,0,'tw1');
        BOOST_CHECK(it->second.cascade_queue.top()->latest_t == 40);
        it->second.cascade_queue.pop();
        BOOST_CHECK(it->second.cascade_queue.top()->latest_t == 70);
        it->second.cascade_queue.pop();
        BOOST_CHECK(it->second.cascade_queue.top()->latest_t == 80);
}

BOOST_AUTO_TEST_CASE( sending_partial_cascades ) {
        tweetoscope::Processors m_p_2(generate_params());
        tweetoscope::tweet twt = tweet_builder("tweet",0,60,0,'tw1');
        m_p_2 += twt;
        for (int i=100; i<600; i+=100) {
            tweetoscope::tweet twt = tweet_builder("retweet",i,60,0,'tw1');
            m_p_2 += twt;
        }
        m_p_2 += tweet_builder("tweet",700,60,0,'tw2');
        auto it = m_p_2.processors.find(0);
        auto it_c = it->second.partial_cascade.find(600);
        auto it_c_2 = it->second.partial_cascade.find(1200);
        BOOST_CHECK(it_c->second.size()==1);
        BOOST_CHECK(it_c_2->second.size()==2);

}

BOOST_AUTO_TEST_CASE( sending_terminated_cascades ) {
        tweetoscope::Processors m_p_3(generate_params());
        tweetoscope::tweet twt = tweet_builder("tweet",0,60,0,'tw1');
        m_p_3 += twt;
        for (int i=100; i<600; i+=100) {
            tweetoscope::tweet twt = tweet_builder("retweet",i,60,0,'tw1');
            m_p_3 += twt;
        }
        auto it = m_p_3.processors.find(0);
        BOOST_CHECK(it->second.cascade_queue.top()->latest_t == 500);
        m_p_3 += tweet_builder("tweet",3000,60,0,'tw2');
        BOOST_CHECK(it->second.cascade_queue.top()->latest_t == 3000);
        auto it_c = it->second.partial_cascade.find(600);
        auto it_c_2 = it->second.partial_cascade.find(1200);
        BOOST_CHECK(it_c->second.size()==1);
        BOOST_CHECK(it_c_2->second.size()==1);
        BOOST_CHECK(it->second.cascade_queue.size()==1);     

}
BOOST_AUTO_TEST_CASE( check_terminated ) {
    tweetoscope::Processors m_p_3(generate_params());
    m_p_3 += tweet_builder("tweet",0,60,0,'tw1');
    for (int i=100; i<600; i+=100) {
        tweetoscope::tweet twt = tweet_builder("retweet",i,60,0,'tw1');
        m_p_3 += twt;
    }
    m_p_3 += tweet_builder("tweet",600,60,0,'tw2');
    for (int i=700; i<1200; i+=100) {
        tweetoscope::tweet twt = tweet_builder("retweet",i,60,0,'tw2');
        m_p_3 += twt;
    }
    m_p_3 += tweet_builder("tweet",2600,60,0,'tw3');
    auto it = m_p_3.processors.find(0);
    BOOST_CHECK(it->second.cascade_queue.size()==2);

}

BOOST_AUTO_TEST_SUITE_END()










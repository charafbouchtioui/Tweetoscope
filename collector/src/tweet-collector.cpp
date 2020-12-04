#include <Collector.hpp>
#include <tweetoscopeCollectorParams.hpp>
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

int main(int argc, char* argv[]) {


  if(argc != 2) {
    std::cout << "Usage : " << argv[0] << " <config-filename>" << std::endl;
    return 0;
  }

  //We parse the parameters
  tweetoscope::params::collector params(argv[1]);
  std::cout << std::endl
        << "Parameters : " << std::endl
        << "----------"    << std::endl
        << std::endl
        << params << std::endl
        << std::endl;



  // We create the kafka consumer and subscribe it to the topic (tweets)
	cppkafka::Configuration config {
	  {"metadata.broker.list", params.kafka.brokers},
    { "auto.offset.reset", "earliest" },
	  {"log.connection.close", false },
    {"group.id","mygroup"}
	};

	cppkafka::Consumer consumer(config);

  
  consumer.subscribe({params.topic.in});

  //Create a Processor Map 
  tweetoscope::Processors processors(argv[1]);


  while(true) {
    // Poll the message 
    auto msg = consumer.poll();

    if( msg && ! msg.get_error() ) {
       tweetoscope::tweet twt;
       // we read the key and value of the message
       auto key = tweetoscope::cascade::idf(std::stoi(msg.get_key()));
       auto istr = std::istringstream(std::string(msg.get_payload()));
       // Create the tweet from the istream object
       istr >> twt;
       twt.key = key;
       // See doumentation of the += operator of the class Processors
       processors += twt;
       // Marking the message as consumed to kafka broker
       consumer.commit(msg);

    }
}

  return 0;
}


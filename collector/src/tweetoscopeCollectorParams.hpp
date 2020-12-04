/**
 * @file tweetoscopeCollectorParams.hpp
 * @brief This file provides a method to parse the parameter file
 * @version 1.0
 * @date 2020-11-25
 * 
 * @copyright Copyright (c) 2020
 * 
 */


#pragma once

#include <tuple>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstddef>
#include <stdexcept>

namespace tweetoscope {

  namespace params {
    namespace section {
		/**
		 * @brief Class to store Kafka Brokers
		 * 
		 */
      struct Kafka {
	std::string brokers;
      };
      /**
       * @brief Class to store Topics for sending partial and terminated cascades
       * 
       */
      struct Topic {
	std::string in, out_series, out_properties;
      };
      /**
       * @brief Class to store The observation windows and the time for termination
       * 
       */
      struct Times {
	std::vector<std::size_t> observation; 
	std::size_t              terminated;
      };
	/**
	 * @brief Storing the minimum number of tweets in a cascade to be considered
	 * 
	 */
      struct Cascade {
	std::size_t min_cascade_size;
      };
    }
    
    /**
     * @brief The class collector parses the parameters from the params.config file
     * 
     */
    struct collector {
    private:
      std::string current_section; /**< Keeping track of the section parsed in the params file */
	/**
	 * @brief This function parses a line key=value in the param file
	 * 
	 * @param is istream object
	 * @return std::pair<std::string, std::string>  pair of stings of the parsed key and value
	 */
      std::pair<std::string, std::string> parse_value(std::istream& is) {
	char c;
	std::string buf;
	is >> std::ws >> c;
	while(c == '#' || c == '[') {
	  if(c == '[') std::getline(is, current_section, ']');
	  std::getline(is, buf, '\n');
	  is >> std::ws >> c;
	}
	is.putback(c);
	std::string key, val;
	is >> std::ws;
	std::getline(is, key, '=');
	is >> val;
	std::getline(is, buf, '\n');
	return {key, val};
      }
      
    public:

      section::Kafka   kafka;
      section::Topic   topic;
      section::Times   times;
      section::Cascade cascade;
	  /**
	   * @brief Default constructor, added for testing purposes
	   * 
	   */
	  collector() = default;
	  /**
	   * @brief Construct a collector object from a params.config file
	   * 
	   * To construct the collector object, the function parse_value() is called
	   * while the it did not reach the end of the file, and with the current_section
	   * and the key extracted, the value parsed is assigned to the corresponding 
	   * member (kafka, times ...)
	   * 
	   * @param config_filename the file path
	   */
      collector(const std::string& config_filename) {
	std::ifstream ifs(config_filename.c_str());
	if(!ifs)
	  throw std::runtime_error(std::string("Cannot open \"") + config_filename + "\" for reading parameters.");
	ifs.exceptions(std::ios::failbit | std::ios::badbit | std::ios::eofbit);
	try {
	  while(true) {
	    auto [key, val] = parse_value(ifs);
	    if(current_section == "kafka") {
	      if(key == "brokers") kafka.brokers = val;
	    }
	    else if(current_section == "topic") {
	      if     (key == "in")             topic.in             = val;
	      else if(key == "out_series")     topic.out_series     = val;
	      else if(key == "out_properties") topic.out_properties = val;
	    }
	    else if(current_section == "times") {
	      if     (key == "observation")    times.observation.push_back(std::stoul(val));
	      else if(key == "terminated")     times.terminated = std::stoul(val);
	    }
	    else if(current_section == "cascade") {
	      if (key == "min_cascade_size")   cascade.min_cascade_size = std::stoul(val);
	    }
	  }
	}
	catch(const std::exception& e) {/* nope, end of file occurred. */}
      }
    };
	/**
	 * @brief Overloading the << operator to streams the params collector object
	 * 
	 * @param os 
	 * @param c params::collector
	 * @return std::ostream& 
	 */
    inline std::ostream& operator<<(std::ostream& os, const collector& c) {
      os << "[kafka]" << std::endl
	 << "  brokers=" << c.kafka.brokers << std::endl
	 << std::endl
	 << "[topic]" << std::endl
	 << "  in=" << c.topic.in << std::endl
	 << "  out_series=" << c.topic.out_series << std::endl
	 << "  out_properties=" << c.topic.out_properties << std::endl
	 << std::endl
	 << "[times]" << std::endl;
      for(auto& o : c.times.observation)
	os << "  observation=" << o << std::endl;
      os << "  terminated=" << c.times.terminated << std::endl
	 << std::endl
	 << "[cascade]" << std::endl
	 << "  min_cascade_size=" << c.cascade.min_cascade_size << std::endl;
	  return os;
    }
  }
}

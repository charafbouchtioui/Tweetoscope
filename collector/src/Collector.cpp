#pragma once
#include <Collector.hpp>

namespace tweetoscope {
    
  
    std::ostream& operator<<(std::ostream& os, const Cascade& c) {
    os << "{\"key\" : "        << c.key         << " , "
       << "\"source_id\" : "   << c.source_id   << " , "
       << "\"msg\" : "         << c.msg         << " , "
       << "\"latest_time\" : " << c.latest_t << " , "
       << "\"list_retweets\" : [";

    for(auto ptr_t = c.tweets.begin(); ptr_t != c.tweets.end(); ++ptr_t){
      os << "{\"time\": "     << ptr_t->first      << " , "
         << "\"magnitude\": " << ptr_t->second << "\"}";
    
      if (ptr_t != c.tweets.end()-1) os << ",";
    }
    os << "]}";
    return os;
    }

    cascade::ref cascade_ptr(const tweet& t) {
      return std::make_shared<Cascade>(std::move(t));
    }

    bool cascade_ref_comparator::operator()(cascade::ref op1, cascade::ref op2) const{
      return *op2 < *op1;
    }

    bool Cascade::operator<(const Cascade& other) const {
      return latest_t < other.latest_t;
    }
    std::string format_cascade_series(const Cascade& c, timestamp obs) {
        std::ostringstream os;
        os << "{\"type\" : "  << "\"serie\""  << " , "
        << "\"cid\" : "    << c.key        << " , "
        << "\"msg\" : "    << c.msg        << " , "
        << "\"T_obs\" : "  << obs          << " , " 
        << "\"tweets\" : [";
        for(auto ptr_t = c.tweets.begin(); ptr_t != c.tweets.end(); ++ptr_t){
        os << "(" << ptr_t->first << ", " << ptr_t->second << ")";
        if (ptr_t != c.tweets.end()-1) os << ",";
        }
        os << "]}";
        return os.str();
    }

    std::string format_cascade_properties(const Cascade& c) {
        std::ostringstream os;
        os << "{\"type\" : "  << "\"size\""    << " , "
        << "\"cid\" : "    << c.key         << " , "
        << "\"n_tot\" : "  << c.tweets.size() << " , "
        << "\"t_end\" : "  << c.latest_t << "}";
        return os.str();
    }

    int get_partition(Processors& pr, timestamp obs) {
      auto it = std::find(pr.parameters.times.observation.begin(), 
                            pr.parameters.times.observation.end(), obs);
      int index = std::distance(pr.parameters.times.observation.begin(), it);
      return index%4;
    }
    
    void send_kafka(cascade::ref c_ptr, Processors& pr, timestamp obs, bool Terminated){
      if (Terminated) { 
        cppkafka::MessageBuilder builder_p {pr.parameters.topic.out_properties};
        auto key = std::to_string(obs);
        builder_p.partition(get_partition(pr,obs));
        builder_p.key(key);
        auto msg = format_cascade_properties(*c_ptr);
        builder_p.payload(msg);
        pr.producer.produce(builder_p);
      } else {
        cppkafka::MessageBuilder builder_s {pr.parameters.topic.out_series};
        auto msg = format_cascade_series(*c_ptr,obs);
        builder_s.payload(msg);
        pr.producer.produce(builder_s);

      }

    }

    Processor::Processor(const tweet& t, params::collector p) : parameters(p) {
    auto r = cascade_ptr(t);
    r->location = cascade_queue.push(r);
    for (auto obs : p.times.observation) {
      partial_cascade.insert({obs, {}});
      partial_cascade[obs].push(r);
    }
    symbols.insert(std::make_pair(t.key, r));
    }

    void Processors::operator+=(const tweet& t) {
        //If the source does not exist, Construct a Processor and insert it in the map
        auto [p_ptr,is_new_source] = processors.try_emplace(t.source,t,this->parameters);
        // Case 2 : Source exists
        if (!is_new_source) {
          auto c_ptr = cascade_ptr(t);
          // Check if the cascade id is already in the symbol table, if not we insert it 
          auto [it_s, is_symbol_created] = p_ptr->second.symbols.insert(std::make_pair(t.key, c_ptr));

          // Partial cascades 
          for(auto& [obs, cascades]: p_ptr->second.partial_cascade){
            while(!cascades.empty()) {
              if (auto sp = cascades.front().lock()) {
                if (t.time - sp->first_t >= obs) {
                  // send the kafka : topic = cascade_series
                  //std::cout << "[cascade_series] Key = None  Values = " <<  format_cascade_series(*sp, obs) << std::endl;
                  // Check if cascade lenght >= min_cascade_size
                  if (sp->tweets.size()>=parameters.cascade.min_cascade_size){
                      send_kafka(sp, *this, obs, false);
                  }
                  cascades.pop();
                } else break; //Since by construction of the queue oldest cascades are in the front, Break as soon as a cascade does not satisfy the condition
              } else cascades.pop(); //delete cascades which are terminated but the weak_pointer is still present in the partial_cascade map
            }
            // new created cascade, so it should be added to all the partial cascades
            if(is_symbol_created) cascades.push(c_ptr);
          }

          ///Queue
          // Test if cascades in the queue verify the condition of termination, Send the kafka message and erase the terminated cascades from the queue
          while(!p_ptr->second.cascade_queue.empty() && this->parameters.times.terminated \
               <= t.time - p_ptr->second.cascade_queue.top()->latest_t) {
            
            auto r = p_ptr->second.cascade_queue.top();
            // send the kafka : topic = cascade_properties
            for (auto obs : this->parameters.times.observation) {
                //std::cout << "[cascade_properties] Key = " << obs << "  Values = " << format_cascade_properties(*r) << std::endl;
                // Check if cascade lenght >= min_cascade_size
                if (r->tweets.size() >= parameters.cascade.min_cascade_size){
                  send_kafka(r, *this, obs, true);
                }
            }
            p_ptr->second.cascade_queue.pop();
          }
        // If the cascade is newly created, push it to the queue
        if (is_symbol_created) c_ptr->location = p_ptr->second.cascade_queue.push(c_ptr);

        //Update queue
        if(auto sp = it_s->second.lock()) {
          // Update the latest time of the cascade
          sp->latest_t = t.time;
          // Only push the tweet if the cascade is not newly created
          if (!is_symbol_created) sp->tweets.push_back(std::make_pair(t.time,t.magnitude)); // push the tweets to the cascade
          // Update the location in the queue (when tweet is added the priority of the cascade changes)
          p_ptr->second.cascade_queue.update(sp->location);
        }
        }

    }


}
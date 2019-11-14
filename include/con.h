#include <vector>
#include <string>
#include <memory>
#include <getopt.h>
#include <csignal>
#include <iostream>
#include "librdkafka/rdkafkacpp.h"
 
class kafka_consumer_client{
public:
    kafka_consumer_client(const std::string& brokers, const std::string& topics, std::string groupid, int64_t offset=-1);
    //kafka_consumer_client();
    virtual ~kafka_consumer_client();
 
    bool initClient();
    bool consume(int timeout_ms);
    void finalize();
private:
    void consumer(RdKafka::Message *msg, void *opt);
 
    std::string brokers_;
    std::string topics_;
    std::string groupid_;
 
    int64_t last_offset_ = 0;
    RdKafka::Consumer *kafka_consumer_ = nullptr;   
    RdKafka::Topic    *topic_          = nullptr;
    int64_t           offset_          = RdKafka::Topic::OFFSET_BEGINNING;
    int32_t           partition_       = 0;
     
};

package com.github.mahmoud.kafka.tutorial_1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumerDemoWithAssignAndSeek {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(consumerDemoWithAssignAndSeek.class.getName());
        String topic="first_topic";
        //No need for group_id

        //create consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        //instead of we use Assign and Seek to replay data or fetch specific messages
        // Assign
        TopicPartition topicPartitionToReadFrom=new TopicPartition(topic,0);
        long offsetToReadFrom=5L;
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));

        //Seek
        consumer.seek(topicPartitionToReadFrom,offsetToReadFrom);

        //pull the data
        int numOfMessageToRead=10;
        boolean keepReading=true;
        int numOfMessageReadSoFar=0;
        while(keepReading){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record:records){
                numOfMessageReadSoFar+=1;
                logger.info("key: "+record.key()+", "+"value: "+record.value());
                logger.info("partition: "+record.partition()+", "+"offset: "+record.offset());
                if(numOfMessageReadSoFar >= numOfMessageToRead){
                    keepReading=false;
                    break;
                }
            }
        }
    }
}

package com.github.mahmoud.kafka.tutorial_1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumerDemo {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(consumerDemo.class.getName());
        String group_id="fourth-g";
        String topic="first_topic";

        //create consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        //Subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //pull the data
        while(true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record:records){
                logger.info("key: "+record.key()+", "+"value: "+record.value());
                logger.info("partition: "+record.partition()+", "+"offset: "+record.offset());
            }
        }
    }
}

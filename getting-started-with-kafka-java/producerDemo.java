package com.github.mahmoud.kafka.tutorial_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producerDemo {
    public static void main(String[] args) {

        //create producer properties
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","Oh god, oh god");

        //Send data
        producer.send(record);

        //flush data
        producer.flush();
        //close producer
        producer.close();
    }
}

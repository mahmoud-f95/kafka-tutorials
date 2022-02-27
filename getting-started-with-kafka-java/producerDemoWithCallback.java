package com.github.mahmoud.kafka.tutorial_1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerDemoWithCallback {
    public static void main(String[] args) {
        final Logger logger= LoggerFactory.getLogger(producerDemoWithCallback.class);
        //create producer properties
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create a producer record
        for(int i=0;i<10;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
                                                                                        "Oh god, oh god"+Integer.toString(i));

            //Send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record is sent
                    if (e == null) {
                        logger.info("Received new metadata \n" + "topic:" + recordMetadata.topic() + "\n"
                                + "partition:" + recordMetadata.partition() + "\n" + "offset:" + recordMetadata.offset() +
                                "\n" + "timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("error while producing: " + e);
                    }
                }
            });
        }

        //flush data
        producer.flush();
        //close producer
        producer.close();
    }
}

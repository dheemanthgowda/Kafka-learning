package com.learn.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerKeys {

    public static void main(String[] args) throws Exception {
        // Create Producer properties
        Logger log =  LoggerFactory.getLogger("ProducerMain");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            // Try with resource : Any object that implements java.lang.AutoCloseable, which includes all objects which implement java.io.Closeable, can be used as a resource.

            for(int i=0; i<10; i++) {
                ProducerRecord<String, String> rec = new ProducerRecord<>("first_topic", "id_" + i%2+1,"Key testing: " + i);
                producer.send(rec, (recordMetadata, e) -> {
                    //executes everytime record is sent successfully or when there is exception
                    if(e != null) {
                        log.error("Error while writing: "+e);
                    } else {
                        log.info("For record key: "+ rec.key() + "\n"
                                + "Partition is:"+ recordMetadata.partition() + "\n"
                                + "Timestamp is:"+ recordMetadata.timestamp() + "\n"
                                + "Offset is:"+ recordMetadata.offset());
                    }
                });
            }
            producer.flush();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}

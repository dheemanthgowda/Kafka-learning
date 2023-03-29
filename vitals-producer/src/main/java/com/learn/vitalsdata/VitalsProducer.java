package com.learn.vitalsdata;

import com.learn.vitalsdata.datamodel.VitalsImmutable;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VitalsProducer {

    static Logger log =  LoggerFactory.getLogger("ProducerMain");
    static Properties properties = new Properties();
    static KafkaProducer<String, String> producer;

    VitalsProducer() {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Safer producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //All configs below is taken care by ENABLE_IDEMPOTENCE_CONFIG, but just for understanding have defined it
        // 1 if version is less than 1.11 to keep data in a partition in order
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        //High throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(30));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64*1024));

        producer = new KafkaProducer<>(properties);
        publishData();
    }

    public static void publishData() {
        try (CSVReader reader = new CSVReader(new FileReader("/Users/dg072881/Documents/data/test_data.csv"))) {
            List<String[]> data = reader.readAll();
            data.remove(0);
            for (String[] datum : data) {
                Thread.sleep(1000);
                sendData(new VitalsImmutable(datum));
            }
        } catch (CsvException | IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //producer.flush() or close() is we don't use try-resource
            producer.close();
        }
    }

    public static void sendData(VitalsImmutable vitalsImmutable) {
        try {
            // Try with resource : Any object that implements java.lang.AutoCloseable, which includes all objects which implement java.io.Closeable, can be used as a resource.
                ProducerRecord<String, String> rec = new ProducerRecord<>("vitals_topic", vitalsImmutable.toString());
                producer.send(rec, (recordMetadata, e) -> {
                    //executes everytime record is sent successfully or when there is exception
                    if(e != null) {
                        log.error("Error while writing: "+e);
                    } else {
                        log.info("For record: "+ rec + "\n"
                                + "Partition is:"+ recordMetadata.partition() + "\n"
                                + "Timestamp is:"+ recordMetadata.timestamp() + "\n"
                                + "Offset is:"+ recordMetadata.offset());
                    }
                });
        } catch (Exception e) {
            log.error("Error while writing to kafka", e);
        }
    }


}

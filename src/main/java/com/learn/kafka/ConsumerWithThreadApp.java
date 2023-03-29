package com.learn.kafka;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithThreadApp {

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger("ConsumerWithThreadApp");
        Properties properties =  new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "My-demo-app-4");
        CountDownLatch latch =  new CountDownLatch(3);
        ConsumerWithThread c1 = new ConsumerWithThread(properties, latch);
        ConsumerWithThread c2 = new ConsumerWithThread(properties, latch);
        ConsumerWithThread c3 = new ConsumerWithThread(properties, latch);
        Thread t1 =  new Thread(c1);
        Thread t2 =  new Thread(c2);
        Thread t3 =  new Thread(c3);
        t1.start();
        t2.start();
        t3.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("Caught shutdown hook");
            c1.shutdown();
            c2.shutdown();
            c3.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.info("Application shut down correctly");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application is interrupted");
        } finally {
            log.info("Application is closing");
        }
    }
}

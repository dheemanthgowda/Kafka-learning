package com.learn.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithThread implements Runnable {

    Logger log = LoggerFactory.getLogger("ConsumerThread");

    private final CountDownLatch latch;
    private final Consumer<String, String> consumer;

    ConsumerWithThread(Properties properties, CountDownLatch latch) {
        this.latch = latch;
        this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList("first_topic"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> rec : records) {
                    log.info("Key: " + rec.key() + "\n" +
                            "Value: " + rec.value() + "\n" +
                            "Offset: " + rec.offset() + "\n" +
                            "Partition: " + rec.partition());
                }
            }

        } catch (WakeupException e) {
            log.info("Received shutdown signal");
        } finally {
            consumer.close();
            latch.countDown();//Tell the driver code we are done with the consumer
        }
    }
    public void shutdown() {
        consumer.wakeup();
    }
}

package com.knoldus.consumer;

import com.knoldus.Greetings;
import com.knoldus.producer.GreetingProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GreetingConsumer {

    private static final Logger log = LoggerFactory.getLogger(GreetingConsumer.class);

    private static final String GREETING_TOPIC = "greeting";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "greeting.consumer");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(GREETING_TOPIC));
        log.info("Consumer started!!!!!!");
        
        while(true){
            ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String , byte[]> records : consumerRecords) {
                try{
                    Greetings greetings = decodeAvroGreeting(records.value());
                    log.info("Consumed Message, key : {}, value : {}", records.key(), greetings.toString());
                }
                catch (Exception e){

                }
                
            }
        }


    }

    public static Greetings decodeAvroGreeting(byte[] array) throws IOException {

        return Greetings.fromByteBuffer(ByteBuffer.wrap(array));
    }
}

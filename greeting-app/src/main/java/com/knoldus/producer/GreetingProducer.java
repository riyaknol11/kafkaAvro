package com.knoldus.producer;

import com.knoldus.Greetings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class GreetingProducer {

    private static final Logger  log = LoggerFactory.getLogger(GreetingProducer.class);

    private static final String GREETING_TOPIC = "greeting";
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());


        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);

        Greetings greeting = buildGreeting("Hello! Avro Schema");

        byte[] value = greeting.toByteBuffer().array();

        ProducerRecord<String, byte[]> producerRecord =
                new ProducerRecord<>(GREETING_TOPIC, value);
        var recordMetaData = producer.send(producerRecord).get();
        log.info("recordMetaData : " + recordMetaData);

    }


    public static Greetings buildGreeting(String greet){
        return Greetings.newBuilder()
                .setGreetings(greet)
                .build();
    }
    }


package com.mydeveloperplanet.mykafkaproducerplanet;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
public class KafkaProducerController {

    private static final String KEY = "FIXED-KEY";

    private static final Map<Integer, String> LEFT;
    static {
        LEFT = new HashMap<>();
        LEFT.put(1, null);
        LEFT.put(3, "A");
        LEFT.put(5, "B");
        LEFT.put(7, null);
        LEFT.put(9, "C");
        LEFT.put(12, null);
        LEFT.put(15, "D");
    }

    private static final Map<Integer, String> RIGHT;
    static {
        RIGHT = new HashMap<>();
        RIGHT.put(2, null);
        RIGHT.put(4, "a");
        RIGHT.put(6, "b");
        RIGHT.put(8, null);
        RIGHT.put(10, "c");
        RIGHT.put(11, null);
        RIGHT.put(13, null);
        RIGHT.put(14, "d");
    }

    @RequestMapping("/sendMessages/")
    public void sendMessages() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 15; i++) {
                // Every 10 seconds send a message
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {}

                if (LEFT.containsKey(i + 1)) {
                    producer.send(new ProducerRecord<String, String>("my-kafka-left-stream-topic", KEY, LEFT.get(i + 1)));
                }
                if (RIGHT.containsKey(i + 1)) {
                    producer.send(new ProducerRecord<String, String>("my-kafka-right-stream-topic", KEY, RIGHT.get(i + 1)));
                }

            }
        } finally {
            producer.close();
        }

    }

    @RequestMapping("/sendKeyedMessages/")
    public void sendKeyedMessages() {

        final Map<Integer, String> LEFT;
        {
            LEFT = new HashMap<>();
            LEFT.put(1, null);
            LEFT.put(2, "A");
            LEFT.put(3, "B");
            LEFT.put(4, null);
            LEFT.put(5, "C");
            LEFT.put(6, null);
            LEFT.put(7, "D");
        }

        final Map<Integer, String> RIGHT;
        {
            RIGHT = new HashMap<>();
            RIGHT.put(1, null);
            RIGHT.put(2, "a");
            RIGHT.put(3, "b");
            RIGHT.put(4, null);
            RIGHT.put(5, "c");
            RIGHT.put(6, null);
            RIGHT.put(7, null);
            RIGHT.put(8, "d");
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 9; i++) {
                // Every 10 seconds send a message
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {}

                if (LEFT.containsKey(i + 1)) {
                    producer.send(new ProducerRecord<String, String>("my-kafka-left-stream-topic", String.valueOf(i), LEFT.get(i + 1)));
                }
                if (RIGHT.containsKey(i + 1)) {
                    producer.send(new ProducerRecord<String, String>("my-kafka-right-stream-topic", String.valueOf(i), RIGHT.get(i + 1)));
                }

            }
        } finally {
            producer.close();
        }

    }


    @RequestMapping("/sendDocumentMessages/")
    public void sendDocumentMessages() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        List<DocumentMetadataMessage> metadataMessages = createDocumentMetadataList();
        metadataMessages.forEach(
                documentMetadataMessage ->
                        producer.send(new ProducerRecord<String, String>(
                                "my-kafka-left-stream-topic",
                                documentMetadataMessage.getKey(),
                                documentMetadataMessage.getMetaData())));

        List<EventMessage> eventMessages = createEventList();
        createEventList().forEach(
                eventMessage ->
                        producer.send(new ProducerRecord<String, String>(
                                "my-kafka-right-stream-topic",
                                eventMessage.getKey(),
                                eventMessage.getEventData())));


    }

    private List<DocumentMetadataMessage> createDocumentMetadataList(){
        List<DocumentMetadataMessage> documentMetadataMessages = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            String strI = String.valueOf(i);
            DocumentMetadataMessage documentMetadataMessage = new DocumentMetadataMessage(String.valueOf(strI), "metadata for document: "+ String.valueOf(i));
            documentMetadataMessages.add(documentMetadataMessage);
        }
        return documentMetadataMessages;
    }

    private List<EventMessage>createEventList(){
        List<EventMessage> eventMessages = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            String strI = String.valueOf(i);
            EventMessage eventMessage = new EventMessage(String.valueOf(strI), "event data for document: "+ String.valueOf(i));
            eventMessages.add(eventMessage);
        }
        return eventMessages;
    }

}

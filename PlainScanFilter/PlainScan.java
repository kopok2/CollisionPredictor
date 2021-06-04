package tech.oleszek;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.awt.*;
import java.awt.geom.Point2D;
import java.util.*;
import java.util.List;


public class PlainScan implements Runnable, ConsumerLoop {
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final List<String> topics;
    private final int id;

    public PlainScan(int id,
                        String groupId,
                        List<String> topics) {
        this.id = id;
        this.topics = topics;
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", groupId);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(consumerProps);
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        producerProps.put("group.id", groupId);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);
    }

    @Override
    public void run() {
        JSONParser jsonParser = new JSONParser();
        double minHeight = -1.5;
        double maxHeight = 20;
        try {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    try {
                        JSONObject obj = (JSONObject) jsonParser.parse(record.value());
                        JSONArray jsonArray = (JSONArray) obj.get("points");
                        ArrayList<Point2D.Double> points = new ArrayList<Point2D.Double>();
                        for(int i = 0; i < jsonArray.size(); i++) {
                            JSONArray point = (JSONArray) jsonArray.get(i);
                            double height = (double)point.get(1);
                            if(height >= minHeight && height <= maxHeight) {
                                points.add(new Point2D.Double((double) point.get(0), (double) point.get(2)));
                            }
                        }
                        System.out.println(this.id + ": " + points.size());
                        String pointsPayload = serializePoints(points);
                        final ProducerRecord<String, String> sendPoints =
                                new ProducerRecord<>("point-plain", "", pointsPayload);

                        producer.send(sendPoints);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    private String serializePoints(ArrayList<Point2D.Double> points) {
        String result = "{\"points\":[";
        ArrayList<String> serializedPoints = new ArrayList<>();
        for(Point2D.Double point : points) {
            serializedPoints.add("[" + point.x + "," + point.y + "]");
        }
        result += String.join(",", serializedPoints);
        result += "]}";
        return result;
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
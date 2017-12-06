package com.onur.kafka.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Onur_Dincol on 12/6/2017.
 */
public class StreamsJsonConverterApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "json-convert-application6");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE); // Exactly once processing!!

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);


        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, JsonNode> sourceTopicData =
                builder.stream(Serdes.String(), jsonSerde, "mysourceTopic");

        KStream<String, JsonNode> targetTopicData = sourceTopicData
                .mapValues(value -> prepareJson(value));
        targetTopicData.to(Serdes.String(), jsonSerde,"myTargetTopic");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static JsonNode prepareJson(JsonNode inputJson){
        try {
            ObjectNode node = (ObjectNode) new ObjectMapper().readTree(JsonOperations.makeJsonFlatten(inputJson.toString()));
            node.put("start_date", TimeOperations.changePSTtoUTC(node.get("start_date").asText()));
            return node;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

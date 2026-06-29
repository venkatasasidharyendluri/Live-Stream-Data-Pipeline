package com.sasi.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.java_websocket.server.WebSocketServer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


// Extends WebSocketServer so it can talk to browsers
public class StreamConsumer extends WebSocketServer {

    // WebSocket Constructor
    public StreamConsumer(int port) {
        super(new InetSocketAddress(port));
    }

    // --- WebSocket Event Handlers ---
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("👀 New Viewer Connected: " + conn.getRemoteSocketAddress());
    }
    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("❌ Viewer Disconnected");
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // We don't expect messages FROM the browser, only TO the browser
    }
    @Override
    public void onError(WebSocket conn, Exception ex) {
        ex.printStackTrace();
    }
    @Override
    public void onStart() {
        System.out.println("🚀 WebSocket Server started on port 8080");
    }

    // --- MAIN METHOD ---
    public static void main(String []args){
        // 1. Start the WebSocket Server on Port 8080
        StreamConsumer socketServer = new StreamConsumer(8080);
        socketServer.start();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"live-chat-group-2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        // 3. Subscribe to the Topic
        consumer.subscribe(Collections.singletonList("youtubecomment"));
        // VALID with 2 topics
//        consumer.subscribe(Arrays.asList("youtube_comments", "other_topics"));

        //  ✔ VALID with regex subscribed to all topics that starts with YouTube
//        consumer.subscribe(Pattern.compile("youtube.*"));
        System.out.println("🎧 Kafka Consumer Started. Waiting for comments...");

        try{
            while (true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord<String,String> record : records){

                    String jsonMessage = record.value();

                    // Print to Console
                    System.out.println("Broadcasting: " + jsonMessage);

                    // Push to ALL connected Browsers via WebSocket
                    socketServer.broadcast(jsonMessage);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }
}
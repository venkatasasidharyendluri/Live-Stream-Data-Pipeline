package com.sasi.Producer;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.*;
import com.google.gson.Gson;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Producer {

    private static final String YOUTUBE_KEY ="your key";
//    https://www.youtube.com/live/Zh1_tKjvOOI?si=_f0AxQpoTndbN5cO
    private static final String VIDEO_ID = "live youtube video id";


    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        Gson gson = new Gson();

        //Telling which address it can find the kafka broker
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //Serialization to the key & value  here the key and value both String
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // --- RELIABILITY SETTINGS  Acknowledgement & IDEMPOTENCE
        props.put(ProducerConfig.ACKS_CONFIG , "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try{
            //creating a YouTube API client that program will use to make YouTube API calls.
             YouTube youtube = new YouTube.Builder(new NetHttpTransport(),
                        new GsonFactory(), httpRequest -> {})
                        .setApplicationName("Live-Chat-Display").build();
             System.out.println("⏳ Connecting to YouTube...");

             String liveChatId = getLiveChatId(youtube, VIDEO_ID);
             System.out.println("✅ Connected! Live Chat ID: " + liveChatId);

            String nextPageToken = null;

             while (true){
                 // i. Asking youtube give messages of live stream
                 YouTube.LiveChatMessages.List request = youtube.liveChatMessages()
                                                                .list(liveChatId, Collections.singletonList("snippet,authorDetails"))
                                                                .setPageToken(nextPageToken)
                                                                .setKey(YOUTUBE_KEY);

                 LiveChatMessageListResponse response = request.execute();

                 // ii. Loop through the messages
                 List<LiveChatMessage> messages = response.getItems();

                 for(LiveChatMessage msg : messages){
                     String author = msg.getAuthorDetails().getDisplayName();
                     String text = msg.getSnippet().getDisplayMessage();
                     long timestamp = msg.getSnippet().getPublishedAt().getValue();

                     // iii. Convert to JSON
                     Comment comment = new Comment(author, text, timestamp);
                     String jsonValue = gson.toJson(comment);

                     // iv. Send to Kafka
                     // We use "youtube_comments" as the topic
                     ProducerRecord<String, String> record =
                             new ProducerRecord<>("youtubecomment", "stream2", jsonValue);
                     producer.send(record);
                     System.out.println("📤 Sent: " + jsonValue);
                 }
                 // v. Update the token so we don't get the same messages again
                 nextPageToken = response.getNextPageToken();

                 // vi. Wait (Respect YouTube's rules)
                 long waitTime = response.getPollingIntervalMillis();
                 System.out.println("😴 Sleeping for " + waitTime + "ms...");
                 Thread.sleep(waitTime);
             }

        }catch (Exception e){
            System.out.println("Exception : "+e);
        }


    }

    // --- HELPER METHODS ---

    // This method asks YouTube: "For this Video ID, what is the Chat ID?"
    private static String getLiveChatId(YouTube youtube, String videoId) throws Exception {
        VideoListResponse response = youtube.videos()
                .list(Collections.singletonList("liveStreamingDetails"))
                .setId(Collections.singletonList(videoId))
                .setKey(YOUTUBE_KEY)
                .execute();

        if (response.getItems().isEmpty()) {
            throw new RuntimeException("Video not found. Check the ID.");
        }

        Video video = response.getItems().get(0);
        VideoLiveStreamingDetails details = video.getLiveStreamingDetails();

        if (details == null || details.getActiveLiveChatId() == null) {
            throw new RuntimeException("This video does not have an active live chat. Is it actually LIVE?");
        }
        return details.getActiveLiveChatId();
    }

    // Simple class to structure our data
    static class Comment {
        String user; String text; long ts;
        public Comment(String u, String t, long time) { this.user=u; this.text=t; this.ts=time; }
    }
}

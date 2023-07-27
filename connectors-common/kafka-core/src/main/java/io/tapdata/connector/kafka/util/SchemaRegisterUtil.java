package io.tapdata.connector.kafka.util;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Author:Skeet
 * Date: 2023/6/30
 **/
public class SchemaRegisterUtil {

    public static int sendHttpRequest(String url) throws IOException {
        HttpURLConnection connection = null;
        BufferedReader reader = null;
        try {
            URL apiUrl = new URL(url);
            connection = (HttpURLConnection) apiUrl.openConnection();
            connection.setRequestMethod("GET");
            int responseCode = connection.getResponseCode();

            // Read the response
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            return responseCode;
        } finally {
            reader.close();
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public static Response sendBasicAuthRequest(String url, String username, String password) throws IOException {
        String user = username + ":" + password;
        String encodedCredentials = Base64.getEncoder().encodeToString(user.getBytes(StandardCharsets.UTF_8));
        OkHttpClient client = new OkHttpClient().newBuilder().build();
        Request request = new Request.Builder()
                .url(url)
                .method("GET",null)
                .header("Authorization", "Basic " + encodedCredentials)
                .build();
        try {
            Response response = client.newCall(request).execute();
            response.close();
            return response;
        } catch (IOException e) {
            throw new IOException(e.getMessage());
        }
    }

    public static StringBuilder httpRequest(String url) throws IOException {
        HttpURLConnection connection = null;
        BufferedReader reader = null;
        try {
            URL apiUrl = new URL(url);
            connection = (HttpURLConnection) apiUrl.openConnection();
            connection.setRequestMethod("GET");
            // Read the response
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            return response;
        } finally {
            if (connection != null) {
                connection.disconnect();
                reader.close();
            }
        }
    }

    public static List<String> parseJsonArray(String jsonArray) {
        List<String> result = new ArrayList<>();
        jsonArray = jsonArray.trim();
        if (jsonArray.startsWith("[") && jsonArray.endsWith("]")) {
            jsonArray = jsonArray.substring(1, jsonArray.length() - 1);
            String[] elements = jsonArray.split(",");
            for (String element : elements) {
                String subject = element.trim().replaceAll("\"", "");
                result.add(subject);
            }
        }
        return result;
    }

    public static boolean checkTopicExists(String bootstrapServers, String topicToCheck) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(properties);
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true);
            ListTopicsResult listTopicsResult = adminClient.listTopics(options);
            KafkaFuture<Map<String, TopicListing>> future = listTopicsResult.namesToListings();
            Map<String, TopicListing> topicListingMap = future.get();
            return topicListingMap.containsKey(topicToCheck);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return false;
    }


}

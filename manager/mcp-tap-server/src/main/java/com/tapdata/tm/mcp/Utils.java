package com.tapdata.tm.mcp;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.servlet.function.ServerRequest;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:16
 */
@Slf4j
public class Utils {
    private static ObjectMapper objectMapper;
    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
        prettyPrinter.indentArraysWith(new DefaultIndenter());
        objectMapper.setDefaultPrettyPrinter(prettyPrinter);
        objectMapper.registerModule(new SimpleModule(){
            @Override
            public void setupModule(SetupContext context) {
                super.setupModule(context);
                SimpleSerializers serializers = new SimpleSerializers();
                serializers.addSerializer(ObjectId.class, new ObjectIdSerialize());
                SimpleDeserializers deserializers = new SimpleDeserializers();
                deserializers.addDeserializer(ObjectId.class, new ObjectIdDeserialize());
                context.addSerializers(serializers);
                context.addDeserializers(deserializers);
            }
        });
    }

    public static String getStringValue(Map<String, Object> params, String key, String defaultValue) {
        return Optional.ofNullable(getStringValue(params, key)).orElse(defaultValue);
    }
    public static String getStringValue(Map<String, Object> params, String key) {
        if (params == null || !params.containsKey(key) || params.get(key) == null)
            return null;
        return params.get(key).toString();
    }

    public static Integer getIntegerValue(Map<String, Object> params, String key) {
        return Optional.ofNullable(params.get(key)).map(v -> {
            if (v instanceof Integer)
                return (Integer)v;
            if (v instanceof String)
                return Integer.parseInt((String)v);
            return null;
        }).orElse(null);
    }
    public static Long getLongValue(Map<String, Object> params, String key) {
        return Optional.ofNullable(params.get(key)).map(v -> {
            if (v instanceof Long)
                return (Long)v;
            if (v instanceof String)
                return Long.parseLong((String)v);
            return null;
        }).orElse(null);
    }

    public static String getAccessCode(ServerRequest request) {
        List<String> authorization = request.headers().header("Authorization");
        if (authorization.isEmpty()) {
            return request.param("accessCode").orElse(null);
        } else {
            String[] tmp = authorization.get(0).split(" ");
            return tmp.length > 1 ? tmp[1] : tmp[0];
        }
    }

    public static String readJsonSchema(String filename) {
        try (InputStream input = Utils.class.getClassLoader().getResourceAsStream(filename);){
            if (input != null)
                return IOUtils.toString(input, StandardCharsets.UTF_8);
            else
                log.error("Not found json schema file {}", filename);
        } catch (Exception e) {
            log.error("Read json schema failed {}", filename);
        }
        return "{\"type\": \"object\"}";
    }

    public static String toJson(Object data) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String sendPostRequest(String url, Object data) throws IOException {

        String postData = objectMapper.writeValueAsString(data);

        URL apiUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

        try {
            connection.setRequestMethod(HttpMethod.POST.name());
            connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/json; utf-8");
            connection.setRequestProperty(HttpHeaders.USER_AGENT, "Java HttpURLConnection");

            connection.setDoOutput(true);
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = postData.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    return response.toString();
                }
            } else {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(connection.getErrorStream(), StandardCharsets.UTF_8))) {
                    StringBuilder errorResponse = new StringBuilder();
                    String errorLine;
                    while ((errorLine = br.readLine()) != null) {
                        errorResponse.append(errorLine.trim());
                    }
                    throw new IOException("HTTP错误: " + responseCode + ", " + errorResponse);
                }
            }
        } finally {
            connection.disconnect();
        }
    }

    public static <T> T parseJson(String json, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    public static <T> T parseJson(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> readConnection(DataSourceEntity ds) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", ds.getId().toHexString());
        data.put("name", ds.getName());
        data.put("databaseType", ds.getDatabase_type());
        data.put("connectionType", ds.getConnection_type());
        data.put("tableCount", ds.getTableCount());
        data.put("loadSchemaTime", ds.getLoadSchemaTime());
        if (ds.getListtags() != null)
            data.put("tags", ds.getListtags().stream().map(tag -> tag.get("value")).collect(Collectors.toList()));
        return data;
    }
}

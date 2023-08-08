package io.tapdata.connector.selectdb.util;

import io.tapdata.connector.selectdb.SelectDbJdbcContext;
import io.tapdata.connector.selectdb.streamload.Constants;
import io.tapdata.connector.selectdb.streamload.HttpPutBuilder;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import okhttp3.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.rmi.server.ExportException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Author:Skeet
 * Date: 2022/12/17
 **/
public class CopyIntoUtils {
    public static final String TAG = CopyIntoUtils.class.getSimpleName();

    private TapConnectionContext tapConnectionContext;
    private static String user;
    private static String password;
    private static String database;
    private static String selectdbHttp;
    private static boolean primaryKeys;
    private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";
    private static final String COMMIT_PATTERN = "http://%s/copy/query";
    private static final HttpClientBuilder httpClientBuilder = HttpClients.custom().disableRedirectHandling();
    private static String uuidName;
    private static String tableName;
    public CopyIntoUtils(TapConnectionContext tapConnectionContext) {
        this.tapConnectionContext = tapConnectionContext;
        setConfig(tapConnectionContext);
    }
    public static void setConfig(TapConnectionContext tapConnectionContext){
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        user = connectionConfig.getString("user");
        password = connectionConfig.getString("password");
        database = connectionConfig.getString("database");
        selectdbHttp = String.format("%s:%s", connectionConfig.getString("host"), connectionConfig.getString("selectDbHttp"));
    }

    public CopyIntoUtils(boolean key) {
        primaryKeys = key;
    }

    public synchronized static void upload(String uuid, byte[] bytes,TapTable table) throws IOException {
        if (selectdbHttp == null) {
            throw new RuntimeException("load_url cannot be empty, or the host cannot connect.Please check your configuration.");
        }
        String uploadLoadUrl = String.format(UPLOAD_URL_PATTERN, selectdbHttp);
        String uploadUuidName =  uuid + ".json";
        uuidName = uploadUuidName;
        String location = getUploadAddress(uploadLoadUrl, uuidName);
        tableName = table.getId();
        put(location, uuidName, bytes);
    }

    public synchronized static Response copyInto() throws IOException {

        OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
        builder.connectTimeout(15, TimeUnit.SECONDS);
        builder.readTimeout(25, TimeUnit.SECONDS);
        OkHttpClient client = builder.build();
        MediaType mediaType = MediaType.parse("application/json");
        String DUPLICATE_KEY_SQL = "{\"sql\": \"copy into " +
                database + "." +
                tableName + " from @~(\\\"" +
                uuidName + "\\\") PROPERTIES (\\\"file.strip_outer_array\\\"=\\\"true\\\",\\\"copy.async\\\"=\\\"false\\\",\\\"file.type\\\"=\\\"json\\\",\\\"file.column_separator\\\"=\\\"" +
                Constants.FIELD_DELIMITER_DEFAULT + "\\\")\"}";
        String UNIQUE_KEY_SQL = "{\"sql\": \"copy into " +
                database + "." +
                tableName + " from @~(\\\"" +
                uuidName + "\\\") PROPERTIES (\\\"file.strip_outer_array\\\"=\\\"true\\\",\\\"copy.use_delete_sign\\\"=\\\"true\\\",\\\"copy.async\\\"=\\\"false\\\",\\\"file.type\\\"=\\\"json\\\",\\\"file.column_separator\\\"=\\\"" +
                Constants.FIELD_DELIMITER_DEFAULT + "\\\")\"}";

        RequestBody body = RequestBody.create(mediaType, primaryKeys ? DUPLICATE_KEY_SQL : UNIQUE_KEY_SQL);
        String uploadLoadUrl = String.format(COMMIT_PATTERN, selectdbHttp);
        final String authInfo = user + ":" + password;
        byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
        Request request = new Request.Builder()
                .url(uploadLoadUrl)
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", "Basic " + new String(encoded))
                .build();
        Response response = client.newCall(request).execute();
        response.close();
//        TapLogger.info(TAG, "CopyInto successfully.");
        return response;
    }

    public static void uploadTest(byte[] bytes) throws IOException {
        if (selectdbHttp == null) {
            throw new RuntimeException("load_url cannot be empty, or the host cannot connect.Please check your configuration.");
        }
        String uploadLoadUrl = String.format(UPLOAD_URL_PATTERN, selectdbHttp);
        String uploadUuidName = UUID.randomUUID().toString() + "_" + System.currentTimeMillis() + ".json";
        uuidName = uploadUuidName;
        String location = getUploadAddress(uploadLoadUrl, uuidName);
        put(location, uuidName, bytes);
    }

    public static void copyIntoTest(String table) throws IOException {

        OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
        builder.connectTimeout(15, TimeUnit.SECONDS);
        builder.readTimeout(25, TimeUnit.SECONDS);
        OkHttpClient client = builder.build();
        MediaType mediaType = MediaType.parse("application/json");
        String DUPLICATE_KEY_SQL = "{\"sql\": \"copy into " +
                database + "." +
                table + " from @~(\\\"" +
                uuidName + "\\\") PROPERTIES (\\\"file.strip_outer_array\\\"=\\\"true\\\",\\\"copy.async\\\"=\\\"false\\\",\\\"file.type\\\"=\\\"json\\\",\\\"file.column_separator\\\"=\\\"" +
                Constants.FIELD_DELIMITER_DEFAULT + "\\\")\"}";
        String UNIQUE_KEY_SQL = "{\"sql\": \"copy into " +
                database + "." +
                table + " from @~(\\\"" +
                uuidName + "\\\") PROPERTIES (\\\"file.strip_outer_array\\\"=\\\"true\\\",\\\"copy.use_delete_sign\\\"=\\\"true\\\",\\\"copy.async\\\"=\\\"false\\\",\\\"file.type\\\"=\\\"json\\\",\\\"file.column_separator\\\"=\\\"" +
                Constants.FIELD_DELIMITER_DEFAULT + "\\\")\"}";

        RequestBody body = RequestBody.create(mediaType, primaryKeys ? DUPLICATE_KEY_SQL : UNIQUE_KEY_SQL);
        String uploadLoadUrl = String.format(COMMIT_PATTERN, selectdbHttp);
        final String authInfo = user + ":" + password;
        byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
        Request request = new Request.Builder()
                .url(uploadLoadUrl)
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", "Basic " + new String(encoded))
                .build();
        Response response = client.newCall(request).execute();
        response.close();
        TapLogger.info(TAG, "CopyInto successfully.");
    }

    private static String getUploadAddress(String loadUrl, String fileName) throws IOException {
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder.setUrl(loadUrl)
                .addFileName(fileName)
                .addCommonHeader()
                .setEmptyEntity()
                .baseAuth(user, password);
        CloseableHttpResponse execute = httpClientBuilder.build().execute(putBuilder.build());
        int statusCode = execute.getStatusLine().getStatusCode();
        if (statusCode == 307) {
            Header location = execute.getFirstHeader("location");
            String uploadAddress = location.getValue();
            return uploadAddress;
        } else {
            HttpEntity entity = execute.getEntity();
            throw new RuntimeException("Could not get the redirected address.");
        }
    }

    public static String put(String loadUrl, String fileName, byte[] data) throws IOException {

        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder.setUrl(loadUrl)
                .addCommonHeader()
                .addFileName(fileName)
                .setEntity(new InputStreamEntity(new ByteArrayInputStream(data)));
        CloseableHttpResponse response = httpClientBuilder.build().execute(putBuilder.build());
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 200 && statusCode < 300) {
            return response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
        } else {
            throw new ExportException("Error code " + statusCode);
        }
    }
}

package io.tapdata.connector.selectdb.streamload;

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
    private static String host;
    private static String user;
    private static String password;
    private static String database;
    private static String port;
    private static String selectdbHttp;
    private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";
    private static final String COMMIT_PATTERN = "http://%s/copy/query";
    private static final HttpClientBuilder httpClientBuilder = HttpClients.custom().disableRedirectHandling();
    private static String uuidName;

    public CopyIntoUtils(TapConnectionContext tapConnectionContext) {
        this.tapConnectionContext = tapConnectionContext;
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        host = connectionConfig.getString("host");
        port = connectionConfig.getString("port");
        user = connectionConfig.getString("user");
        password = connectionConfig.getString("password");
        database = connectionConfig.getString("database");
        selectdbHttp = connectionConfig.getString("selectdbHttp");
    }

    public static void upload(byte[] bytes) throws IOException {
        if (selectdbHttp == null) {
            throw new RuntimeException("load_url cannot be empty, or the host cannot connect.Please check your configuration.");
        }
        String uploadLoadUrl = String.format(UPLOAD_URL_PATTERN, selectdbHttp);
        String uploadUuidName = UUID.randomUUID().toString() + "_" + System.currentTimeMillis() + ".csv";
        uuidName = uploadUuidName;
        String location = getUploadAddress(uploadLoadUrl, uuidName);
        put(location, uuidName, bytes);
    }

    public static void copyInto(TapTable table) throws IOException {

        OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
        builder.connectTimeout(15, TimeUnit.SECONDS);
        builder.readTimeout(25, TimeUnit.SECONDS);
        OkHttpClient client = builder.build();
        MediaType mediaType = MediaType.parse("application/json");
        String sql = "{\"sql\": \"copy into " +
                database + "." +
                table.getId() + " from @~(\\\"" +
                uuidName + "\\\") PROPERTIES (\\\"copy.use_delete_sign\\\"=\\\"true\\\",\\\"copy.async\\\"=\\\"false\\\",\\\"file.type\\\"=\\\"csv\\\",\\\"file.column_separator\\\"=\\\"" +
                Constants.FIELD_DELIMITER_DEFAULT + "\\\")\"}";
        RequestBody body = RequestBody.create(mediaType, sql);
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

    public CopyIntoUtils setHost(String host) {
        this.host = host;
        return this;
    }

    public CopyIntoUtils setUser(String user) {
        this.user = user;
        return this;
    }

    public CopyIntoUtils setPassword(String password) {
        this.password = password;
        return this;
    }


}

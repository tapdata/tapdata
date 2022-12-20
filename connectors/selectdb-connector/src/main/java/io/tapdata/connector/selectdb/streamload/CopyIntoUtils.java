package io.tapdata.connector.selectdb.streamload;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import okhttp3.*;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
    private boolean loadBatchFirstRecord;
    private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";
    private static final String COMMIT_PATTERN = "http://%s/copy/query";
    private static final HttpClientBuilder httpClientBuilder = HttpClients.custom().disableRedirectHandling();
    private static final String uuidName = UUID.randomUUID().toString() + "_" + System.currentTimeMillis() + ".csv";

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
        String location = getUploadAddress(uploadLoadUrl, uuidName);
        put(location, uuidName, bytes);
    }

    public static void copyInto(TapTable table) throws IOException {
//        String host = getLoadHost();
//        if (host == null) {
//            throw new RuntimeException("load_url cannot be empty, or the host cannot connect.Please check your configuration.");
//        }
//        OkHttpClient client = new OkHttpClient().newBuilder().build();
//        MediaType mediaType = MediaType.parse("application/json");
//        String sql = "{\"sql\": \"copy into" + table.getId() + "from @~(\\\"" + uuidName + "\\\") PROPERTIES (\\\"copy.async\\\"=\\\"false\\\",\\\"file.type\\\"=\\\"csv\\\",\\\"file.column_separator\\\"=\\\",\\\")\\\"}";
//
//        RequestBody body = RequestBody.create(mediaType, sql);
//        String queryLoadUrl = String.format(COMMIT_PATTERN, selectdbHttp);
//        Request request = new Request.Builder()
//                .url(queryLoadUrl)
//                .method("POST", body)
//                .addHeader("Content-Type", "application/json")
//                .addHeader("Authorization", "Basic YWRtaW46R290YXBkOA==")
//                .build();
//        Response response = client.newCall(request).execute();
//        System.out.println(response);

        OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
        builder.connectTimeout(10, TimeUnit.SECONDS);
        builder.readTimeout(10, TimeUnit.SECONDS);
        OkHttpClient client = builder.build();

        MediaType mediaType = MediaType.parse("application/json");
        String sql = "{\"sql\": \"copy into " + database + "." + table.getId() + " from @~(\\\"" + uuidName + "\\\") PROPERTIES (\\\"copy.async\\\"=\\\"false\\\",\\\"file.type\\\"=\\\"csv\\\",\\\"file.column_separator\\\"=\\\",\\\")\"}";
        RequestBody body = RequestBody.create(mediaType, sql);
        Request request = new Request.Builder()
                .url("http://39.108.6.77:42199/copy/query")
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", "Basic YWRtaW46R290YXBkOA==")
                .build();
        Response response = client.newCall(request).execute();
        response.close();
        TapLogger.info(TAG, "");


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


//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//        byteArrayOutputStream.write(data);
//        byteArrayOutputStream.write(IOUtils.toByteArray(FileUtils.openInputStream(new File(""))));
//        byte[] newData = byteArrayOutputStream.toByteArray();

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

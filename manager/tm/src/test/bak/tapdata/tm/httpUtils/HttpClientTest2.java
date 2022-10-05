package bak.tapdata.tm.httpUtils;

import org.apache.commons.codec.CharEncoding;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HttpClientTest2 {


    @Test
    public void testGet() {
        String url = "http://localhost:8080/api/Connections/listAll";
        //参数绑定
        BufferedReader bufferedReader = null;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            StringBuilder urlBuilder = new StringBuilder(url);
            urlBuilder.append("?").append("access_token=").append("461ea92ff87749dba041a87c285d795f8d7fbe70c7f449a983dc7bf2ab522b9e");
            String param = "{\"fields\":{\"id\":true,\"name\":true,\"connection_type\":true,\"status\":true,\"user_id\":true,\"database_type\":true},\"where\":{\"or\":[{\"connection_type\":\"source_and_target\"},{\"connection_type\":\"target\"}]}}";
            urlBuilder.append("&filter=").append(URLEncoder.encode(param, StandardCharsets.UTF_8.name()));
//        HttpPost httpPost = new HttpPost(url);
            HttpGet httpGet = new HttpGet(urlBuilder.toString());
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .setSocketTimeout(15000).build();
            httpGet.setConfig(requestConfig);


            CloseableHttpResponse response = httpClient.execute(httpGet);
            StatusLine statusLine = response.getStatusLine();
            int statusCode = statusLine.getStatusCode();
            if (statusCode < HttpStatus.SC_OK || statusCode >= HttpStatus.SC_MULTIPLE_CHOICES) {
                throw new RuntimeException("request url:" + url + " fail,status code is : " + statusCode);
            }

            HttpEntity entity = response.getEntity();
            String body = EntityUtils.toString(entity, CharEncoding.UTF_8);
            System.out.println("body:" + body);
//            InputStream content = response.getEntity().getContent();
//            bufferedReader = new BufferedReader(new InputStreamReader(content, StandardCharsets.UTF_8.name()));
//            String line;
//            StringBuilder result = new StringBuilder();
//            while ((line = bufferedReader.readLine()) != null) {
//                result.append(line);
//            }
//            System.out.println("结果为：" + result.toString());

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void main(String[] args) {
        String url = "http://36.134.131.197:3080/api/iz0znun2tu2";
        String access_token = "eyJraWQiOiI5YWZjZjhkMS1iY2FhLTRmZTMtYjQ0Yy03ZjQ0ZDQ3NDMzOGIiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjbGllbnRJZCI6IjVjMGU3NTBiN2E1Y2Q0MjQ2NGE1MDk5ZCIsInJvbGVzIjpbIiRldmVyeW9uZSIsImFkbWluIl0sImlzcyI6Imh0dHA6XC9cLzEyNy4wLjAuMTozMDAwIiwiZXhwaXJlZGF0ZSI6MTY2MzgyNzE1NzQwMCwiYXVkIjoiNWMwZTc1MGI3YTVjZDQyNDY0YTUwOTlkIiwiY3JlYXRlZEF0IjoxNjYyNjE3NTU3NDAwLCJuYmYiOjE2NjI2MTc1NTcsInNjb3BlIjpbImFkbWluIl0sImV4cCI6MTY2MzgyNzE1NywiaWF0IjoxNjYyNjE3NTU3LCJqdGkiOiJkNzY2YTYzNy0xNGE3LTRmYjEtYmFiOS01NTM4Y2I3ODhhZjUifQ.TOWRGiLFg9XaSlu4y22inpfJDCB0lZo4VcUUl6iI4bq3HlMyX9C33k0lFlHutlXLWKGaEmC-AA6rGb40iim93rKmFPuYAgHqjtomyZTjYgWzW5FV-M4p5Ma47j70TewUlI45oLPm6-beTUW4pDzzfqutBDWc5ait15jCwSWcYoRwRvHzPXoLmzU6WmacaReaox5rec9rhYyqAYA8KZ2BR84R5dVlA02jritUEqZUXeMYmhpD_4_pJRNnT4tyR-Ln2nZkpjPul8SsUyqJjp6ue4T_PFwzTWSM2WX-EXu7WmoBgV67nfMl0yXoVIsi8JsCYJS9w8eS1QZ97VPMmT5qMA";
        String param = "{\"page\": 1,\"limit\": 20}";
        String result1 = doGet(url, access_token, param);
        System.out.println(result1);
        String result2 = doPost(url+"/find", access_token, param);
        System.out.println(result2);
    }


    /**
     * @param url
     * @param access_token
     * @param param
     * @return
     */
    public static String doGet(String url, String access_token, String param) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            StringBuilder urlBuilder = new StringBuilder(url);
            urlBuilder.append("?access_token=").append(access_token);
            urlBuilder.append("&filter=").append(URLEncoder.encode(param, StandardCharsets.UTF_8.name()));
            HttpGet httpGet = new HttpGet(urlBuilder.toString());
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .setSocketTimeout(15000).build();
            httpGet.setConfig(requestConfig);

            CloseableHttpResponse response = httpClient.execute(httpGet);
            StatusLine statusLine = response.getStatusLine();
            int statusCode = statusLine.getStatusCode();
            if (statusCode < HttpStatus.SC_OK || statusCode >= HttpStatus.SC_MULTIPLE_CHOICES) {
                throw new RuntimeException("request url:" + url + " fail,status code is : " + statusCode);
            }
            HttpEntity entity = response.getEntity();
            if (entity != null)
                return EntityUtils.toString(entity, CharEncoding.UTF_8);
            else
                throw new RuntimeException("request url:" + url + " fail,status code is : " + statusLine.getStatusCode());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     *
     * @param url
     * @param access_token
     * @param reqBody
     * @return
     */
    public static String doPost(String url, String access_token, String reqBody) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            StringBuilder urlBuilder = new StringBuilder(url);
            urlBuilder.append("?access_token=").append(access_token);
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .setSocketTimeout(15000).build();
            HttpPost httpPost = new HttpPost(urlBuilder.toString());
            httpPost.setHeader("content-type", "application/json;charset=UTF-8");
            httpPost.setConfig(requestConfig);
            httpPost.setEntity(new StringEntity(reqBody, Charset.defaultCharset()));


            CloseableHttpResponse response = httpClient.execute(httpPost);
            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                return EntityUtils.toString(entity, CharEncoding.UTF_8);
            } else {
                throw new RuntimeException("request url:" + url + " fail,status code is : " + statusLine.getStatusCode());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testPatch() {
        String url = "http://localhost:8080/api/Modules";
        //参数绑定
        try {
            StringBuilder urlBuilder = new StringBuilder(url);
            urlBuilder.append("?").append("access_token=").append("4cfd73f1eb254876955a0bc11bbb45615da0c218940f4758810935384cf8746f");
            //请求体
            String reqBody = "{\"datasource\":\"62f86de2737c5026a54aab3a\",\"tablename\":\"CAR_CLAIM\",\"readPreference\":\"\",\"readPreferenceTag\":\"\",\"readConcern\":\"\",\"apiVersion\":\"v1\",\"name\":\"mysql\",\"describtion\":\"\",\"prefix\":\"\",\"basePath\":\"mysql_2019_155446\",\"path\":\"/api/v1/mysql_2019_155446\",\"apiType\":\"defaultApi\",\"status\":\"active\",\"createType\":\"\",\"paths\":[{\"path\":\"/api/v1/mysql_2019_155446\",\"method\":\"POST\",\"description\":\"创建新记录\",\"name\":\"create\",\"result\":\"Document\",\"type\":\"preset\",\"acl\":[\"admin\"]},{\"path\":\"/api/v1/mysql_2019_155446/{id}\",\"method\":\"GET\",\"description\":\"根据id获取记录\",\"name\":\"findById\",\"params\":[{\"name\":\"id\",\"type\":\"string\",\"defaultvalue\":1,\"description\":\"document id\"}],\"result\":\"Document\",\"type\":\"preset\",\"acl\":[\"admin\"]},{\"path\":\"/api/v1/mysql_2019_155446/{id}\",\"method\":\"PATCH\",\"name\":\"updateById\",\"params\":[{\"name\":\"id\",\"type\":\"string\",\"defaultvalue\":1,\"description\":\"document id\"}],\"description\":\"根据id更新记录\",\"result\":\"Document\",\"type\":\"preset\",\"acl\":[\"admin\"]},{\"path\":\"/api/v1/mysql_2019_155446/{id}\",\"method\":\"DELETE\",\"name\":\"deleteById\",\"params\":[{\"name\":\"id\",\"type\":\"string\",\"description\":\"document id\"}],\"description\":\"根据id删除记录\",\"type\":\"preset\",\"acl\":[\"admin\"]},{\"path\":\"/api/v1/mysql_2019_155446\",\"method\":\"GET\",\"name\":\"findPage\",\"params\":[{\"name\":\"page\",\"type\":\"int\",\"defaultvalue\":1,\"description\":\"page number\"},{\"name\":\"limit\",\"type\":\"int\",\"defaultvalue\":20,\"description\":\"max records per page\"},{\"name\":\"sort\",\"type\":\"object\",\"description\":\"sort setting,Array ,format like [{'propertyName':'ASC'}]\"},{\"name\":\"filter\",\"type\":\"object\",\"description\":\"search filter object,Array\"}],\"description\":\"分页获取记录\",\"result\":\"Page<Document>\",\"type\":\"preset\",\"acl\":[\"admin\"]}],\"listtags\":[],\"id\":\"630de3d953d0eb7364099bff\",\"createAt\":\"2022-08-30T10:18:01.902+00:00\",\"lastUpdAt\":\"2022-08-30T10:57:03.452+00:00\",\"userId\":\"62bc5008d4958d013d97c7a6\",\"lastUpdBy\":\"62bc5008d4958d013d97c7a6\",\"fields\":[{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac42\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":1,\"tapType\":\"{\\\"byteRatio\\\":4,\\\"bytes\\\":12,\\\"defaultValue\\\":1,\\\"type\\\":10}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"varchar(12)\",\"default_value\":null,\"field_name\":\"CLAIM_ID\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":false,\"original_field_name\":\"CLAIM_ID\",\"primaryKey\":true,\"primary_key_position\":1},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac43\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":2,\"tapType\":\"{\\\"byteRatio\\\":4,\\\"bytes\\\":12,\\\"defaultValue\\\":1,\\\"type\\\":10}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"varchar(12)\",\"default_value\":null,\"field_name\":\"POLICY_ID\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"POLICY_ID\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac44\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":3,\"tapType\":\"{\\\"defaultFraction\\\":0,\\\"fraction\\\":0,\\\"max\\\":\\\"9999-12-31T23:59:59.999999Z\\\",\\\"min\\\":\\\"1000-01-01T00:00:00Z\\\",\\\"type\\\":1}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"datetime\",\"default_value\":null,\"field_name\":\"CLAIM_DATE\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"CLAIM_DATE\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac45\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":4,\"tapType\":\"{\\\"defaultFraction\\\":0,\\\"fraction\\\":0,\\\"max\\\":\\\"9999-12-31T23:59:59.999999Z\\\",\\\"min\\\":\\\"1000-01-01T00:00:00Z\\\",\\\"type\\\":1}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"datetime\",\"default_value\":null,\"field_name\":\"SETTLED_DATE\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"SETTLED_DATE\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac46\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":5,\"tapType\":\"{\\\"fixed\\\":true,\\\"maxValue\\\":999999999999999999999999999999,\\\"minValue\\\":-999999999999999999999999999999,\\\"precision\\\":30,\\\"scale\\\":2,\\\"type\\\":8}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"decimal(30,2)\",\"default_value\":null,\"field_name\":\"CLAIM_AMOUNT\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"CLAIM_AMOUNT\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac47\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":6,\"tapType\":\"{\\\"fixed\\\":true,\\\"maxValue\\\":999999999999999999999999999999,\\\"minValue\\\":-999999999999999999999999999999,\\\"precision\\\":30,\\\"scale\\\":2,\\\"type\\\":8}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"decimal(30,2)\",\"default_value\":null,\"field_name\":\"SETTLED_AMOUNT\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"SETTLED_AMOUNT\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac48\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":7,\"tapType\":\"{\\\"byteRatio\\\":4,\\\"bytes\\\":30,\\\"defaultValue\\\":1,\\\"type\\\":10}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"varchar(30)\",\"default_value\":null,\"field_name\":\"CLAIM_REASON\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"CLAIM_REASON\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac49\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":8,\"tapType\":\"{\\\"defaultFraction\\\":0,\\\"fraction\\\":6,\\\"max\\\":\\\"9999-12-31T23:59:59.999999Z\\\",\\\"min\\\":\\\"1000-01-01T00:00:00Z\\\",\\\"type\\\":1}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"datetime(6)\",\"default_value\":null,\"field_name\":\"LAST_CHANGE\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"LAST_CHANGE\",\"primaryKey\":false}],\"connection\":\"62f86de2737c5026a54aab3a\",\"failRate\":0,\"source\":{\"id\":\"62f86de2737c5026a54aab3a\",\"lastUpdBy\":\"62bc5008d4958d013d97c7a6\",\"name\":\"mysql-183-33062\",\"config\":{\"timezone\":\"\",\"username\":\"root\",\"password\":\"Gotapd8\",\"port\":33062,\"host\":\"192.168.1.183\",\"addtionalString\":\"\",\"database\":\"INSURANCE\"},\"connection_type\":\"source_and_target\",\"database_type\":\"Mysql\",\"definitionScope\":\"public\",\"definitionVersion\":\"1.0-SNAPSHOT\",\"definitionGroup\":\"io.tapdata\",\"definitionPdkId\":\"mysql\",\"definitionBuildNumber\":\"null\",\"pdkType\":\"pdk\",\"pdkHash\":\"a5af410b12afca476edf4a650c133ddf135bf76542a67787ed6f7f7d53ba712\",\"capabilities\":[{\"type\":10,\"id\":\"new_field_event\"},{\"type\":10,\"id\":\"alter_field_name_event\"},{\"type\":10,\"id\":\"alter_field_attributes_event\"},{\"type\":10,\"id\":\"drop_field_event\"},{\"type\":11,\"id\":\"release_external_function\"},{\"type\":11,\"id\":\"batch_read_function\"},{\"type\":11,\"id\":\"stream_read_function\"},{\"type\":11,\"id\":\"batch_count_function\"},{\"type\":11,\"id\":\"timestamp_to_stream_offset_function\"},{\"type\":11,\"id\":\"write_record_function\"},{\"type\":11,\"id\":\"query_by_advance_filter_function\"},{\"type\":11,\"id\":\"create_table_function\"},{\"type\":11,\"id\":\"clear_table_function\"},{\"type\":11,\"id\":\"drop_table_function\"},{\"type\":11,\"id\":\"create_index_function\"},{\"type\":11,\"id\":\"alter_field_attributes_function\"},{\"type\":11,\"id\":\"alter_field_name_function\"},{\"type\":11,\"id\":\"drop_field_function\"},{\"type\":11,\"id\":\"new_field_function\"},{\"type\":11,\"id\":\"get_table_names_function\"},{\"type\":20,\"id\":\"dml_insert_policy\",\"alternatives\":[\"update_on_exists\",\"ignore_on_exists\"]},{\"type\":20,\"id\":\"dml_update_policy\",\"alternatives\":[\"ignore_on_nonexists\",\"insert_on_nonexists\"]}],\"retry\":0,\"everLoadSchema\":true,\"schemaVersion\":\"76dce6c4-7141-4a3e-bf2a-590d7bb90f27\",\"status\":\"ready\",\"tableCount\":286,\"testTime\":1660540276366,\"project\":\"\",\"transformed\":true,\"schema\":{},\"loadCount\":288,\"loadFieldsStatus\":\"finished\",\"submit\":true,\"loadSchemaField\":false,\"loadFieldErrMsg\":\"\",\"kafkaConsumerRequestTimeout\":0,\"kafkaConsumerUseTransactional\":false,\"kafkaMaxPollRecords\":0,\"kafkaPollTimeoutMS\":0,\"kafkaMaxFetchBytes\":0,\"kafkaMaxFetchWaitMS\":0,\"kafkaIgnoreInvalidRecord\":false,\"kafkaProducerRequestTimeout\":0,\"kafkaProducerUseTransactional\":false,\"kafkaRetries\":0,\"kafkaBatchSize\":0,\"kafkaAcks\":\"-1\",\"kafkaLingerMS\":0,\"kafkaDeliveryTimeoutMS\":0,\"kafkaMaxRequestSize\":0,\"kafkaMaxBlockMS\":0,\"kafkaBufferMemory\":0,\"kafkaCompressionType\":\"\",\"kafkaPartitionKey\":\"\",\"kafkaIgnorePushError\":false,\"agentTags\":[],\"shareCdcEnable\":false,\"accessNodeType\":\"AUTOMATIC_PLATFORM_ALLOCATION\",\"accessNodeProcessId\":\"\",\"accessNodeProcessIdList\":[],\"accessNodeTypeEmpty\":false,\"createTime\":\"2022-08-14T03:37:06.958+00:00\",\"last_updated\":\"2022-08-15T05:11:18.371+00:00\",\"user_id\":\"62bc5008d4958d013d97c7a6\"}}";

            CloseableHttpClient httpClient = HttpClients.createDefault();
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .setSocketTimeout(15000).build();
            HttpPatch httpPatch = new HttpPatch(urlBuilder.toString());
            httpPatch.setHeader("content-type","application/json;charset=UTF-8");
            httpPatch.setConfig(requestConfig);
            httpPatch.setEntity(new StringEntity(reqBody));


            CloseableHttpResponse response = httpClient.execute(httpPatch);
            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String body = EntityUtils.toString(entity, CharEncoding.UTF_8);
                System.out.println("body:" + body);
            }else{
                throw new RuntimeException("request url:" + url + " fail,status code is : " + statusLine.getStatusCode());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    @Test
    public void testPost() {
        String url = "http://localhost:8080/api/Modules";
        //参数绑定
        try(CloseableHttpClient httpClient = HttpClients.createDefault()) {
            StringBuilder urlBuilder = new StringBuilder(url);
            urlBuilder.append("?").append("access_token=").append("4cfd73f1eb254876955a0bc11bbb45615da0c218940f4758810935384cf8746f");
            //请求体
            String reqBody = "{\"datasource\":\"62f86de2737c5026a54aab3a\",\"tablename\":\"CAR_CLAIM\",\"readPreference\":\"secondary\",\"readPreferenceTag\":\"\",\"readConcern\":\"\",\"apiVersion\":\"v1\",\"name\":\"新增模型测试2\",\"describtion\":\"\",\"prefix\":\"\",\"basePath\":\"mysql_add3\",\"path\":\"/api/v1/mysql_add2\",\"apiType\":\"defaultApi\",\"status\":\"pending\",\"createType\":\"\",\"paths\":[{\"path\":\"/api/v1/mysql_add\",\"method\":\"POST\",\"description\":\"创建新记录\",\"name\":\"create\",\"result\":\"Document\",\"type\":\"preset\",\"acl\":[\"admin\"]},{\"path\":\"/api/v1/mysql_add/{id}\",\"method\":\"GET\",\"description\":\"根据id获取记录\",\"name\":\"findById\",\"params\":[{\"name\":\"id\",\"type\":\"string\",\"defaultvalue\":1,\"description\":\"document id\"}],\"result\":\"Document\",\"type\":\"preset\",\"acl\":[\"admin\"]},{\"path\":\"/api/v1/mysql_add/{id}\",\"method\":\"PATCH\",\"name\":\"updateById\",\"params\":[{\"name\":\"id\",\"type\":\"string\",\"defaultvalue\":1,\"description\":\"document id\"}],\"description\":\"根据id更新记录\",\"result\":\"Document\",\"type\":\"preset\",\"acl\":[\"admin\"]},{\"path\":\"/api/v1/mysql_add/{id}\",\"method\":\"DELETE\",\"name\":\"deleteById\",\"params\":[{\"name\":\"id\",\"type\":\"string\",\"description\":\"document id\"}],\"description\":\"根据id删除记录\",\"type\":\"preset\",\"acl\":[\"admin\"]},{\"path\":\"/api/v1/mysql_add\",\"method\":\"GET\",\"name\":\"findPage\",\"params\":[{\"name\":\"page\",\"type\":\"int\",\"defaultvalue\":1,\"description\":\"page number\"},{\"name\":\"limit\",\"type\":\"int\",\"defaultvalue\":20,\"description\":\"max records per page\"},{\"name\":\"sort\",\"type\":\"object\",\"description\":\"sort setting,Array ,format like [{'propertyName':'ASC'}]\"},{\"name\":\"filter\",\"type\":\"object\",\"description\":\"search filter object,Array\"}],\"description\":\"分页获取记录\",\"result\":\"Page<Document>\",\"type\":\"preset\",\"acl\":[\"admin\"]}],\"listtags\":[],\"fields\":[{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac42\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":1,\"tapType\":\"{\\\"byteRatio\\\":4,\\\"bytes\\\":12,\\\"defaultValue\\\":1,\\\"type\\\":10}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"varchar(12)\",\"default_value\":null,\"field_name\":\"CLAIM_ID\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":false,\"original_field_name\":\"CLAIM_ID\",\"primaryKey\":true,\"primary_key_position\":1},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac43\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":2,\"tapType\":\"{\\\"byteRatio\\\":4,\\\"bytes\\\":12,\\\"defaultValue\\\":1,\\\"type\\\":10}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"varchar(12)\",\"default_value\":null,\"field_name\":\"POLICY_ID\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"POLICY_ID\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac44\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":3,\"tapType\":\"{\\\"defaultFraction\\\":0,\\\"fraction\\\":0,\\\"max\\\":\\\"9999-12-31T23:59:59.999999Z\\\",\\\"min\\\":\\\"1000-01-01T00:00:00Z\\\",\\\"type\\\":1}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"datetime\",\"default_value\":null,\"field_name\":\"CLAIM_DATE\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"CLAIM_DATE\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac45\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":4,\"tapType\":\"{\\\"defaultFraction\\\":0,\\\"fraction\\\":0,\\\"max\\\":\\\"9999-12-31T23:59:59.999999Z\\\",\\\"min\\\":\\\"1000-01-01T00:00:00Z\\\",\\\"type\\\":1}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"datetime\",\"default_value\":null,\"field_name\":\"SETTLED_DATE\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"SETTLED_DATE\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac46\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":5,\"tapType\":\"{\\\"fixed\\\":true,\\\"maxValue\\\":999999999999999999999999999999,\\\"minValue\\\":-999999999999999999999999999999,\\\"precision\\\":30,\\\"scale\\\":2,\\\"type\\\":8}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"decimal(30,2)\",\"default_value\":null,\"field_name\":\"CLAIM_AMOUNT\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"CLAIM_AMOUNT\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac47\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":6,\"tapType\":\"{\\\"fixed\\\":true,\\\"maxValue\\\":999999999999999999999999999999,\\\"minValue\\\":-999999999999999999999999999999,\\\"precision\\\":30,\\\"scale\\\":2,\\\"type\\\":8}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"decimal(30,2)\",\"default_value\":null,\"field_name\":\"SETTLED_AMOUNT\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"SETTLED_AMOUNT\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac48\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":7,\"tapType\":\"{\\\"byteRatio\\\":4,\\\"bytes\\\":30,\\\"defaultValue\\\":1,\\\"type\\\":10}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"varchar(30)\",\"default_value\":null,\"field_name\":\"CLAIM_REASON\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"CLAIM_REASON\",\"primaryKey\":false},{\"autoincrement\":\"NO\",\"id\":\"62f86dff737c5026a54aac49\",\"source\":\"auto\",\"unique\":false,\"columnPosition\":8,\"tapType\":\"{\\\"defaultFraction\\\":0,\\\"fraction\\\":6,\\\"max\\\":\\\"9999-12-31T23:59:59.999999Z\\\",\\\"min\\\":\\\"1000-01-01T00:00:00Z\\\",\\\"type\\\":1}\",\"sourceDbType\":\"Mysql\",\"useDefaultValue\":true,\"data_type\":\"datetime(6)\",\"default_value\":null,\"field_name\":\"LAST_CHANGE\",\"is_auto_allowed\":true,\"is_deleted\":false,\"is_nullable\":true,\"original_field_name\":\"LAST_CHANGE\",\"primaryKey\":false}]}";


            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .setSocketTimeout(15000).build();
            HttpPost httpPost = new HttpPost(urlBuilder.toString());
            httpPost.setHeader("content-type","application/json;charset=UTF-8");
            httpPost.setConfig(requestConfig);
            httpPost.setEntity(new StringEntity(reqBody, Charset.defaultCharset()));


            CloseableHttpResponse response = httpClient.execute(httpPost);
            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String body = EntityUtils.toString(entity, CharEncoding.UTF_8);
                System.out.println("body:" + body);
            }else{
                throw new RuntimeException("request url:" + url + " fail,status code is : " + statusLine.getStatusCode());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test3() {
        String str = "    public void testPatch() {\n" +
                "        String url = \"http://localhost:8080/api/Modules\";\n" +
                "        //参数绑定\n" +
                "        BufferedReader bufferedReader = null;\n" +
                "        try {\n" +
                "            StringBuilder urlBuilder = new StringBuilder(url);\n" +
                "            urlBuilder.append(\"?\").append(\"access_token=\").append(\"4cfd73f1eb254876955a0bc11bbb45615da0c218940f4758810935384cf8746f\");\n" +
                "            //请求体\n" +
                "            String reqBody = \"{\\\"datasource\\\":\\\"62f86de2737c5026a54aab3a\\\",\\\"tablename\\\":\\\"CAR_CLAIM\\\",\\\"readPreference\\\":\\\"\\\",\\\"readPreferenceTag\\\":\\\"\\\",\\\"readConcern\\\":\\\"\\\",\\\"apiVersion\\\":\\\"v1\\\",\\\"name\\\":\\\"mysql\\\",\\\"describtion\\\":\\\"\\\",\\\"prefix\\\":\\\"\\\",\\\"basePath\\\":\\\"mysql_2019_155447\\\",\\\"path\\\":\\\"/api/v1/mysql_2019_155446\\\",\\\"apiType\\\":\\\"defaultApi\\\",\\\"status\\\":\\\"active\\\",\\\"createType\\\":\\\"\\\",\\\"paths\\\":[{\\\"path\\\":\\\"/api/v1/mysql_2019_155446\\\",\\\"method\\\":\\\"POST\\\",\\\"description\\\":\\\"创建新记录\\\",\\\"name\\\":\\\"create\\\",\\\"result\\\":\\\"Document\\\",\\\"type\\\":\\\"preset\\\",\\\"acl\\\":[\\\"admin\\\"]},{\\\"path\\\":\\\"/api/v1/mysql_2019_155446/{id}\\\",\\\"method\\\":\\\"GET\\\",\\\"description\\\":\\\"根据id获取记录\\\",\\\"name\\\":\\\"findById\\\",\\\"params\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\",\\\"defaultvalue\\\":1,\\\"description\\\":\\\"document id\\\"}],\\\"result\\\":\\\"Document\\\",\\\"type\\\":\\\"preset\\\",\\\"acl\\\":[\\\"admin\\\"]},{\\\"path\\\":\\\"/api/v1/mysql_2019_155446/{id}\\\",\\\"method\\\":\\\"PATCH\\\",\\\"name\\\":\\\"updateById\\\",\\\"params\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\",\\\"defaultvalue\\\":1,\\\"description\\\":\\\"document id\\\"}],\\\"description\\\":\\\"根据id更新记录\\\",\\\"result\\\":\\\"Document\\\",\\\"type\\\":\\\"preset\\\",\\\"acl\\\":[\\\"admin\\\"]},{\\\"path\\\":\\\"/api/v1/mysql_2019_155446/{id}\\\",\\\"method\\\":\\\"DELETE\\\",\\\"name\\\":\\\"deleteById\\\",\\\"params\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\",\\\"description\\\":\\\"document id\\\"}],\\\"description\\\":\\\"根据id删除记录\\\",\\\"type\\\":\\\"preset\\\",\\\"acl\\\":[\\\"admin\\\"]},{\\\"path\\\":\\\"/api/v1/mysql_2019_155446\\\",\\\"method\\\":\\\"GET\\\",\\\"name\\\":\\\"findPage\\\",\\\"params\\\":[{\\\"name\\\":\\\"page\\\",\\\"type\\\":\\\"int\\\",\\\"defaultvalue\\\":1,\\\"description\\\":\\\"page number\\\"},{\\\"name\\\":\\\"limit\\\",\\\"type\\\":\\\"int\\\",\\\"defaultvalue\\\":20,\\\"description\\\":\\\"max records per page\\\"},{\\\"name\\\":\\\"sort\\\",\\\"type\\\":\\\"object\\\",\\\"description\\\":\\\"sort setting,Array ,format like [{'propertyName':'ASC'}]\\\"},{\\\"name\\\":\\\"filter\\\",\\\"type\\\":\\\"object\\\",\\\"description\\\":\\\"search filter object,Array\\\"}],\\\"description\\\":\\\"分页获取记录\\\",\\\"result\\\":\\\"Page<Document>\\\",\\\"type\\\":\\\"preset\\\",\\\"acl\\\":[\\\"admin\\\"]}],\\\"listtags\\\":[],\\\"id\\\":\\\"630de3d953d0eb7364099bff\\\",\\\"createAt\\\":\\\"2022-08-30T10:18:01.902+00:00\\\",\\\"lastUpdAt\\\":\\\"2022-08-30T10:57:03.452+00:00\\\",\\\"userId\\\":\\\"62bc5008d4958d013d97c7a6\\\",\\\"lastUpdBy\\\":\\\"62bc5008d4958d013d97c7a6\\\",\\\"fields\\\":[{\\\"autoincrement\\\":\\\"NO\\\",\\\"id\\\":\\\"62f86dff737c5026a54aac42\\\",\\\"source\\\":\\\"auto\\\",\\\"unique\\\":false,\\\"columnPosition\\\":1,\\\"tapType\\\":\\\"{\\\\\\\"byteRatio\\\\\\\":4,\\\\\\\"bytes\\\\\\\":12,\\\\\\\"defaultValue\\\\\\\":1,\\\\\\\"type\\\\\\\":10}\\\",\\\"sourceDbType\\\":\\\"Mysql\\\",\\\"useDefaultValue\\\":true,\\\"data_type\\\":\\\"varchar(12)\\\",\\\"default_value\\\":null,\\\"field_name\\\":\\\"CLAIM_ID\\\",\\\"is_auto_allowed\\\":true,\\\"is_deleted\\\":false,\\\"is_nullable\\\":false,\\\"original_field_name\\\":\\\"CLAIM_ID\\\",\\\"primaryKey\\\":true,\\\"primary_key_position\\\":1},{\\\"autoincrement\\\":\\\"NO\\\",\\\"id\\\":\\\"62f86dff737c5026a54aac43\\\",\\\"source\\\":\\\"auto\\\",\\\"unique\\\":false,\\\"columnPosition\\\":2,\\\"tapType\\\":\\\"{\\\\\\\"byteRatio\\\\\\\":4,\\\\\\\"bytes\\\\\\\":12,\\\\\\\"defaultValue\\\\\\\":1,\\\\\\\"type\\\\\\\":10}\\\",\\\"sourceDbType\\\":\\\"Mysql\\\",\\\"useDefaultValue\\\":true,\\\"data_type\\\":\\\"varchar(12)\\\",\\\"default_value\\\":null,\\\"field_name\\\":\\\"POLICY_ID\\\",\\\"is_auto_allowed\\\":true,\\\"is_deleted\\\":false,\\\"is_nullable\\\":true,\\\"original_field_name\\\":\\\"POLICY_ID\\\",\\\"primaryKey\\\":false},{\\\"autoincrement\\\":\\\"NO\\\",\\\"id\\\":\\\"62f86dff737c5026a54aac44\\\",\\\"source\\\":\\\"auto\\\",\\\"unique\\\":false,\\\"columnPosition\\\":3,\\\"tapType\\\":\\\"{\\\\\\\"defaultFraction\\\\\\\":0,\\\\\\\"fraction\\\\\\\":0,\\\\\\\"max\\\\\\\":\\\\\\\"9999-12-31T23:59:59.999999Z\\\\\\\",\\\\\\\"min\\\\\\\":\\\\\\\"1000-01-01T00:00:00Z\\\\\\\",\\\\\\\"type\\\\\\\":1}\\\",\\\"sourceDbType\\\":\\\"Mysql\\\",\\\"useDefaultValue\\\":true,\\\"data_type\\\":\\\"datetime\\\",\\\"default_value\\\":null,\\\"field_name\\\":\\\"CLAIM_DATE\\\",\\\"is_auto_allowed\\\":true,\\\"is_deleted\\\":false,\\\"is_nullable\\\":true,\\\"original_field_name\\\":\\\"CLAIM_DATE\\\",\\\"primaryKey\\\":false},{\\\"autoincrement\\\":\\\"NO\\\",\\\"id\\\":\\\"62f86dff737c5026a54aac45\\\",\\\"source\\\":\\\"auto\\\",\\\"unique\\\":false,\\\"columnPosition\\\":4,\\\"tapType\\\":\\\"{\\\\\\\"defaultFraction\\\\\\\":0,\\\\\\\"fraction\\\\\\\":0,\\\\\\\"max\\\\\\\":\\\\\\\"9999-12-31T23:59:59.999999Z\\\\\\\",\\\\\\\"min\\\\\\\":\\\\\\\"1000-01-01T00:00:00Z\\\\\\\",\\\\\\\"type\\\\\\\":1}\\\",\\\"sourceDbType\\\":\\\"Mysql\\\",\\\"useDefaultValue\\\":true,\\\"data_type\\\":\\\"datetime\\\",\\\"default_value\\\":null,\\\"field_name\\\":\\\"SETTLED_DATE\\\",\\\"is_auto_allowed\\\":true,\\\"is_deleted\\\":false,\\\"is_nullable\\\":true,\\\"original_field_name\\\":\\\"SETTLED_DATE\\\",\\\"primaryKey\\\":false},{\\\"autoincrement\\\":\\\"NO\\\",\\\"id\\\":\\\"62f86dff737c5026a54aac46\\\",\\\"source\\\":\\\"auto\\\",\\\"unique\\\":false,\\\"columnPosition\\\":5,\\\"tapType\\\":\\\"{\\\\\\\"fixed\\\\\\\":true,\\\\\\\"maxValue\\\\\\\":999999999999999999999999999999,\\\\\\\"minValue\\\\\\\":-999999999999999999999999999999,\\\\\\\"precision\\\\\\\":30,\\\\\\\"scale\\\\\\\":2,\\\\\\\"type\\\\\\\":8}\\\",\\\"sourceDbType\\\":\\\"Mysql\\\",\\\"useDefaultValue\\\":true,\\\"data_type\\\":\\\"decimal(30,2)\\\",\\\"default_value\\\":null,\\\"field_name\\\":\\\"CLAIM_AMOUNT\\\",\\\"is_auto_allowed\\\":true,\\\"is_deleted\\\":false,\\\"is_nullable\\\":true,\\\"original_field_name\\\":\\\"CLAIM_AMOUNT\\\",\\\"primaryKey\\\":false},{\\\"autoincrement\\\":\\\"NO\\\",\\\"id\\\":\\\"62f86dff737c5026a54aac47\\\",\\\"source\\\":\\\"auto\\\",\\\"unique\\\":false,\\\"columnPosition\\\":6,\\\"tapType\\\":\\\"{\\\\\\\"fixed\\\\\\\":true,\\\\\\\"maxValue\\\\\\\":999999999999999999999999999999,\\\\\\\"minValue\\\\\\\":-999999999999999999999999999999,\\\\\\\"precision\\\\\\\":30,\\\\\\\"scale\\\\\\\":2,\\\\\\\"type\\\\\\\":8}\\\",\\\"sourceDbType\\\":\\\"Mysql\\\",\\\"useDefaultValue\\\":true,\\\"data_type\\\":\\\"decimal(30,2)\\\",\\\"default_value\\\":null,\\\"field_name\\\":\\\"SETTLED_AMOUNT\\\",\\\"is_auto_allowed\\\":true,\\\"is_deleted\\\":false,\\\"is_nullable\\\":true,\\\"original_field_name\\\":\\\"SETTLED_AMOUNT\\\",\\\"primaryKey\\\":false},{\\\"autoincrement\\\":\\\"NO\\\",\\\"id\\\":\\\"62f86dff737c5026a54aac48\\\",\\\"source\\\":\\\"auto\\\",\\\"unique\\\":false,\\\"columnPosition\\\":7,\\\"tapType\\\":\\\"{\\\\\\\"byteRatio\\\\\\\":4,\\\\\\\"bytes\\\\\\\":30,\\\\\\\"defaultValue\\\\\\\":1,\\\\\\\"type\\\\\\\":10}\\\",\\\"sourceDbType\\\":\\\"Mysql\\\",\\\"useDefaultValue\\\":true,\\\"data_type\\\":\\\"varchar(30)\\\",\\\"default_value\\\":null,\\\"field_name\\\":\\\"CLAIM_REASON\\\",\\\"is_auto_allowed\\\":true,\\\"is_deleted\\\":false,\\\"is_nullable\\\":true,\\\"original_field_name\\\":\\\"CLAIM_REASON\\\",\\\"primaryKey\\\":false},{\\\"autoincrement\\\":\\\"NO\\\",\\\"id\\\":\\\"62f86dff737c5026a54aac49\\\",\\\"source\\\":\\\"auto\\\",\\\"unique\\\":false,\\\"columnPosition\\\":8,\\\"tapType\\\":\\\"{\\\\\\\"defaultFraction\\\\\\\":0,\\\\\\\"fraction\\\\\\\":6,\\\\\\\"max\\\\\\\":\\\\\\\"9999-12-31T23:59:59.999999Z\\\\\\\",\\\\\\\"min\\\\\\\":\\\\\\\"1000-01-01T00:00:00Z\\\\\\\",\\\\\\\"type\\\\\\\":1}\\\",\\\"sourceDbType\\\":\\\"Mysql\\\",\\\"useDefaultValue\\\":true,\\\"data_type\\\":\\\"datetime(6)\\\",\\\"default_value\\\":null,\\\"field_name\\\":\\\"LAST_CHANGE\\\",\\\"is_auto_allowed\\\":true,\\\"is_deleted\\\":false,\\\"is_nullable\\\":true,\\\"original_field_name\\\":\\\"LAST_CHANGE\\\",\\\"primaryKey\\\":false}],\\\"connection\\\":\\\"62f86de2737c5026a54aab3a\\\",\\\"failRate\\\":0,\\\"source\\\":{\\\"id\\\":\\\"62f86de2737c5026a54aab3a\\\",\\\"lastUpdBy\\\":\\\"62bc5008d4958d013d97c7a6\\\",\\\"name\\\":\\\"mysql-183-33062\\\",\\\"config\\\":{\\\"timezone\\\":\\\"\\\",\\\"username\\\":\\\"root\\\",\\\"password\\\":\\\"Gotapd8\\\",\\\"port\\\":33062,\\\"host\\\":\\\"192.168.1.183\\\",\\\"addtionalString\\\":\\\"\\\",\\\"database\\\":\\\"INSURANCE\\\"},\\\"connection_type\\\":\\\"source_and_target\\\",\\\"database_type\\\":\\\"Mysql\\\",\\\"definitionScope\\\":\\\"public\\\",\\\"definitionVersion\\\":\\\"1.0-SNAPSHOT\\\",\\\"definitionGroup\\\":\\\"io.tapdata\\\",\\\"definitionPdkId\\\":\\\"mysql\\\",\\\"definitionBuildNumber\\\":\\\"null\\\",\\\"pdkType\\\":\\\"pdk\\\",\\\"pdkHash\\\":\\\"a5af410b12afca476edf4a650c133ddf135bf76542a67787ed6f7f7d53ba712\\\",\\\"capabilities\\\":[{\\\"type\\\":10,\\\"id\\\":\\\"new_field_event\\\"},{\\\"type\\\":10,\\\"id\\\":\\\"alter_field_name_event\\\"},{\\\"type\\\":10,\\\"id\\\":\\\"alter_field_attributes_event\\\"},{\\\"type\\\":10,\\\"id\\\":\\\"drop_field_event\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"release_external_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"batch_read_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"stream_read_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"batch_count_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"timestamp_to_stream_offset_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"write_record_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"query_by_advance_filter_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"create_table_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"clear_table_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"drop_table_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"create_index_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"alter_field_attributes_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"alter_field_name_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"drop_field_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"new_field_function\\\"},{\\\"type\\\":11,\\\"id\\\":\\\"get_table_names_function\\\"},{\\\"type\\\":20,\\\"id\\\":\\\"dml_insert_policy\\\",\\\"alternatives\\\":[\\\"update_on_exists\\\",\\\"ignore_on_exists\\\"]},{\\\"type\\\":20,\\\"id\\\":\\\"dml_update_policy\\\",\\\"alternatives\\\":[\\\"ignore_on_nonexists\\\",\\\"insert_on_nonexists\\\"]}],\\\"retry\\\":0,\\\"everLoadSchema\\\":true,\\\"schemaVersion\\\":\\\"76dce6c4-7141-4a3e-bf2a-590d7bb90f27\\\",\\\"status\\\":\\\"ready\\\",\\\"tableCount\\\":286,\\\"testTime\\\":1660540276366,\\\"project\\\":\\\"\\\",\\\"transformed\\\":true,\\\"schema\\\":{},\\\"loadCount\\\":288,\\\"loadFieldsStatus\\\":\\\"finished\\\",\\\"submit\\\":true,\\\"loadSchemaField\\\":false,\\\"loadFieldErrMsg\\\":\\\"\\\",\\\"kafkaConsumerRequestTimeout\\\":0,\\\"kafkaConsumerUseTransactional\\\":false,\\\"kafkaMaxPollRecords\\\":0,\\\"kafkaPollTimeoutMS\\\":0,\\\"kafkaMaxFetchBytes\\\":0,\\\"kafkaMaxFetchWaitMS\\\":0,\\\"kafkaIgnoreInvalidRecord\\\":false,\\\"kafkaProducerRequestTimeout\\\":0,\\\"kafkaProducerUseTransactional\\\":false,\\\"kafkaRetries\\\":0,\\\"kafkaBatchSize\\\":0,\\\"kafkaAcks\\\":\\\"-1\\\",\\\"kafkaLingerMS\\\":0,\\\"kafkaDeliveryTimeoutMS\\\":0,\\\"kafkaMaxRequestSize\\\":0,\\\"kafkaMaxBlockMS\\\":0,\\\"kafkaBufferMemory\\\":0,\\\"kafkaCompressionType\\\":\\\"\\\",\\\"kafkaPartitionKey\\\":\\\"\\\",\\\"kafkaIgnorePushError\\\":false,\\\"agentTags\\\":[],\\\"shareCdcEnable\\\":false,\\\"accessNodeType\\\":\\\"AUTOMATIC_PLATFORM_ALLOCATION\\\",\\\"accessNodeProcessId\\\":\\\"\\\",\\\"accessNodeProcessIdList\\\":[],\\\"accessNodeTypeEmpty\\\":false,\\\"createTime\\\":\\\"2022-08-14T03:37:06.958+00:00\\\",\\\"last_updated\\\":\\\"2022-08-15T05:11:18.371+00:00\\\",\\\"user_id\\\":\\\"62bc5008d4958d013d97c7a6\\\"}}\";\n" +
                "\n" +
                "            CloseableHttpClient httpClient = HttpClients.createDefault();\n" +
                "            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)\n" +
                "                    .setConnectionRequestTimeout(5000)\n" +
                "                    .setSocketTimeout(15000).build();\n" +
                "            HttpPatch httpPatch = new HttpPatch(urlBuilder.toString());\n" +
                "            httpPatch.setHeader(\"content-type\",\"application/json;charset=UTF-8\");\n" +
                "            httpPatch.setConfig(requestConfig);\n" +
                "            httpPatch.setEntity(new StringEntity(reqBody));\n" +
                "\n" +
                "\n" +
                "            CloseableHttpResponse response = httpClient.execute(httpPatch);\n" +
                "            StatusLine statusLine = response.getStatusLine();\n" +
                "            HttpEntity entity = response.getEntity();\n" +
                "            if (entity != null) {\n" +
                "                String body = EntityUtils.toString(entity, CharEncoding.UTF_8);\n" +
                "                System.out.println(\"body:\" + body);\n" +
                "            }else{\n" +
                "                throw new RuntimeException(\"request url:\" + url + \" fail,status code is : \" + statusLine.getStatusCode());\n" +
                "            }\n" +
                "        } catch (IOException e) {\n" +
                "            throw new RuntimeException(e);\n" +
                "        }finally {\n" +
                "            if (bufferedReader != null) {\n" +
                "                try {\n" +
                "                    bufferedReader.close();\n" +
                "                } catch (IOException e) {\n" +
                "                    throw new RuntimeException(e);\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }";

        System.out.println(str);
    }
}

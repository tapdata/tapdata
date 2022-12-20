package io.tapdata.connector.selectdb.streamload;

import cn.hutool.core.lang.Assert;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Author:Skeet
 * Date: 2022/12/14
 **/
public class HttpPutBuilder {
    public Map<String, String> header;
    String url;
    HttpEntity httpEntity;

    public HttpPutBuilder() {
        header = new HashMap<>();
    }

    public HttpPutBuilder setUrl(String url) {
        this.url = url;
        return this;
    }

    public HttpPutBuilder addCommonHeader() {
        header.put(HttpHeaders.EXPECT, "100-continue");
        header.put("column_separator", Constants.FIELD_DELIMITER_DEFAULT);
        return this;
    }

    public HttpPutBuilder addColumns(List<String> columns) {
        List<String> columnList = new ArrayList<>();
        if (header.containsKey("columns")) {
            columnList.addAll(Arrays.asList(header.get("columns").split(",")));
        }
        columnList.addAll(columns);
        header.put("columns", String.join(",", columnList));
        return this;
    }

    public HttpPutBuilder enableDelete() {
        header.put("merge_type", "MERGE");
        header.put("delete", String.format("%s=1", Constants.SELECTDB_DELETE_SIGN));
        return this;
    }

    public HttpPutBuilder enable2PC() {
        header.put("two_phase_commit", "true");
        return this;
    }

    public HttpPutBuilder baseAuth(String user, String password) {
        if (password == null) {
            password = "";
        }
        final String authInfo = user + ":" + password;
        byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
        header.put(HttpHeaders.AUTHORIZATION, "Basic " + new String(encoded));
        return this;
    }

    public HttpPutBuilder addTxnId(long txnID) {
        header.put("txn_id", String.valueOf(txnID));
        return this;
    }

    public HttpPutBuilder commit() {
        header.put("txn_operation", "commit");
        return this;
    }

    public HttpPutBuilder abort() {
        header.put("txn_operation", "abort");
        return this;
    }

    public HttpPutBuilder setEntity(HttpEntity httpEntity) {
        this.httpEntity = httpEntity;
        return this;
    }

    public HttpPutBuilder setEmptyEntity() {
        try {
            this.httpEntity = new StringEntity("");
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    public HttpPutBuilder addProperties(Properties properties) {
        properties.forEach((key, value) -> header.put(String.valueOf(key), String.valueOf(value)));
        return this;
    }

    public HttpPutBuilder setLabel(String label) {
        header.put("label", label);
        return this;
    }

    public HttpPut build() {
        Assert.notNull(url, "url of HttpPutBuilder should never be null");
        Assert.notNull(httpEntity, "httpEntity of HttpPutBuilder should never be null");
        HttpPut put = new HttpPut(url);
        header.forEach(put::setHeader);
        put.setEntity(httpEntity);
        return put;
    }

    public HttpPutBuilder addFileName(String fileName){
        header.put("fileName", fileName);
        return this;
    }
}

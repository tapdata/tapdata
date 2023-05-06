package io.tapdata.connector.doris.streamload;

import io.tapdata.connector.doris.DorisJdbcContext;
import io.tapdata.connector.doris.bean.DorisConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class HttpPutBuilder {
    String url;
    Map<String, String> header;
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
        return this;
    }

    public HttpPutBuilder addHeader(String key, String value) {
        header.put(key, value);
        return this;
    }

    public HttpPutBuilder addFormat(DorisConfig.WriteFormat writeFormat) {
        header.put("format", writeFormat.name());
        switch (writeFormat) {
            case csv:
                header.put("column_separator", Constants.FIELD_DELIMITER_DEFAULT);
                header.put("line_delimiter", Constants.LINE_DELIMITER_DEFAULT);
                break;
            case json:
                header.put("strip_outer_array", "true");
                header.put("fuzzy_parse", "true");
                break;
        }
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
        header.put("delete", String.format("%s=1", Constants.DORIS_DELETE_SIGN));
        return this;
    }

    public HttpPutBuilder enableAppend() {
        header.put("merge_type", "APPEND");
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
        // TODO: check duplicate key.
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
}

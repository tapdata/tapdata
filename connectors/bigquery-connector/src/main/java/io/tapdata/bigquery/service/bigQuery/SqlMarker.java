package io.tapdata.bigquery.service.bigQuery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqlMarker {
    private static final String TAG = SqlMarker.class.getSimpleName();

    private GoogleCredentials credentials;
    private BigQuery bigQuery;

    private SqlMarker(String credentialsJson) {
        try {
            this.credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8)));
            this.bigQuery();
        } catch (IOException e) {
            TapLogger.error(TAG, "Unable to create a connection through Service Account Credentials. Please check whether the Service Account is correct or invalid. ");
        }
    }

    private SqlMarker(GoogleCredentials credentials) {
        this.credentials = credentials;
        this.bigQuery();
    }

    public static SqlMarker create(String credentialsJson) {
        return new SqlMarker(credentialsJson);
    }

    public static SqlMarker create(GoogleCredentials credentials) {
        return new SqlMarker(credentials);
    }

    public List<BigQueryResult> execute(String... sql) {
        List<BigQueryResult> results = new ArrayList<>();
        try {
            for (String subSql : sql) {
                if (Objects.isNull(subSql)) continue;
                results.add(this.execute(subSql));
            }
        } catch (BigQueryException | InterruptedException e) {
            TapLogger.error(TAG, "Big Query execute error,{} , {}", e.getMessage(), sql);
        }
        return results;
    }

    public BigQueryResult executeOnce(String sql) {
        try {
            BigQueryResult execute = execute(sql);
            TapLogger.info(TAG, "BigQuery current operation has executed Sql once, Please pay attention to the upper limit of SQL usage (1500/h), executing Sql: " + sql);
            return execute;
        } catch (BigQueryException | InterruptedException e) {
            throw new CoreException(String.format("%s Big Query execute error, %s , %s.", TAG, e.getMessage(), sql));
        }
    }

    private BigQueryResult execute(String sql) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        try {
            return BigQueryResult.create(this.bigQuery.query(queryConfig));
        } catch (Exception e) {
            throw new CoreException(e.getMessage());
        }
    }

    private void bigQuery() {
        this.bigQuery = BigQueryOptions.newBuilder().setCredentials(this.credentials).build().getService();
    }

    public BigQuery query() {
        return this.bigQuery;
    }
}

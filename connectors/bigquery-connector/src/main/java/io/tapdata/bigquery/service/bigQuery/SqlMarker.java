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

public class SqlMarker{
    private static final String TAG = SqlMarker.class.getSimpleName();

    GoogleCredentials credentials;
    BigQuery bigQuery;
    private SqlMarker(String credentialsJson){
        try {
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8)));
            this.credentials = credentials;
            bigQuery();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private SqlMarker(GoogleCredentials credentials){
        this.credentials = credentials;
        bigQuery();
    }
    public static SqlMarker create(String credentialsJson){
        return new SqlMarker(credentialsJson);
    }

    public static SqlMarker create(GoogleCredentials credentials){
        return new SqlMarker(credentials);
    }

    public List<BigQueryResult> execute(String ...sql){
        List<BigQueryResult> results = new ArrayList<>();
        try {
            for (String subSql : sql) {
                if (null == subSql) continue;
                BigQueryResult execute = execute(subSql);
                if (null!=execute) results.add(execute);
            }
            //results.iterateAll().forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));
            //System.out.println("Query performed successfully.");
        } catch (BigQueryException | InterruptedException e) {
            //System.out.println("Insert operation not performed \n" + e.toString());
            TapLogger.error(TAG,"Big Query execute error,{} , {}",e.getMessage(),sql);
        }
        return results;
    }

    public BigQueryResult executeOnce(String sql){
        try {
            return execute(sql);
        }catch (BigQueryException | InterruptedException e){
            TapLogger.error(TAG,"Big Query execute error,{} , {}",e.getMessage(),sql);
            throw new CoreException(String.format("%s Big Query execute error,%s , %s",TAG,e.getMessage(),sql));
        }catch (Exception e){
            throw e;
        }
    }

    private BigQueryResult execute(String sql) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        try {
            return BigQueryResult.create(this.bigQuery.query(queryConfig));
        }catch (Exception e){
            throw new CoreException(e.getMessage());
        }
    }

    private void bigQuery(){
        this.bigQuery = BigQueryOptions.newBuilder().setCredentials(this.credentials).build().getService();
    }

    public BigQuery query(){
        return this.bigQuery;
    }

}

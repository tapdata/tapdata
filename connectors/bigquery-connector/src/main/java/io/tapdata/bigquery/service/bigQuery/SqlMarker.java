package io.tapdata.bigquery.service.bigQuery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

public class SqlMarker {
    GoogleCredentials credentials;
    BigQuery bigQuery;
    private SqlMarker(String credentialsJson){
        try {
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes("utf8")));
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

    public List<BigQueryResult> excute(String ...sql){
        List<BigQueryResult> results = new ArrayList<>();
        try {
            for (String subSql : sql) {
                if (null == subSql) continue;
                BigQueryResult excute = excute(subSql);
                if (null!=excute) results.add(excute);
            }
            //results.iterateAll().forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));
            //System.out.println("Query performed successfully.");
        } catch (BigQueryException | InterruptedException e) {
            //System.out.println("Insert operation not performed \n" + e.toString());
        }
        return results;
    }

    public BigQueryResult excuteOnce(String sql){
        try {
            return excute(sql);
        }catch (BigQueryException | InterruptedException e){

        }
        return null;
    }

    private BigQueryResult excute(String sql) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        return BigQueryResult.create(this.bigQuery.query(queryConfig));
    }

    private void bigQuery(){
        this.bigQuery = BigQueryOptions.newBuilder().setCredentials(this.credentials).build().getService();
    }

}

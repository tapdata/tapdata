package io.tapdata.bigquery.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import java.nio.file.Path;
import java.nio.file.Paths;

// Sample to query in a table
public class Query {

  public static void main(String[] args) {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "vibrant-castle-366614";
    String datasetName = "tableSet001";//"MY_DATASET_NAME";
    String tableName = "table1";//"MY_TABLE_NAME";
    String query =
        "SELECT * \n"
            + " FROM `"
            + projectId
            + "."
            + datasetName
            + "."
            + tableName
            + "`";
    query(projectId,query);
  }

  public static void query(String projectId,String query) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
//      Path credentialsPath = Paths.get("path/to/your/client_secret.json");
      //newBuilder().setProjectId(projectId).build()
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

      TableResult results = bigquery.query(queryConfig);

      results
          .iterateAll()
          .forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));

      System.out.println("Query performed successfully.");
    } catch (BigQueryException | InterruptedException e) {
      System.out.println("Query not performed \n" + e.toString());
    }
  }
}
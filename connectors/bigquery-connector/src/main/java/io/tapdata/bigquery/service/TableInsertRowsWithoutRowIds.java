package io.tapdata.bigquery.service;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Sample to insert rows without row ids in a table
public class TableInsertRowsWithoutRowIds {

  public static void main(String[] args) {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "vibrant-castle-366614";
    String datasetName = "tableSet001";//"MY_DATASET_NAME";
    String tableName = "table1";//"MY_TABLE_NAME";
    tableInsertRowsWithoutRowIds(projectId,datasetName, tableName);
  }

  public static void tableInsertRowsWithoutRowIds(String projectId,String datasetName, String tableName) {
    String str = "{\n" +
            "  \"type\": \"service_account\",\n" +
            "  \"project_id\": \"vibrant-castle-366614\",\n" +
            "  \"private_key_id\": \"77b6e2d287f1c39a2654d3c4d55168c4f794451f\",\n" +
            "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDRWF/wJBNFA1QA\\nZ6cp6aT5gGTYn5V6URbn92Y1oYHzW7eW+CSNgFr/VTYrFamD2AmQyea1IRC/2f7x\\nXXaaiE+CB9bRURhE78G0Ada40lUt5OSbj/3UsI1Y/82h6DCOgoiZC3GXBPpGW4xN\\nUj3CkTLzhwUwWqn0F/OS9Trkh8PcyPrus8OidnF/5Si0cNTQaNZIvYfVFV2/DhOD\\nCIRuxrYUf58ftvna9qN6eUHgOtcwff5D4jgOC4oP4l6dKdGxzBGvCoQY2kbrwKZJ\\n+HCuBh9J3H0XCRN91/mDvKfJnUf9dqYDPgR232frwPe1MwigM8R37yeMx+R6ZI9J\\nA8WI9rGbAgMBAAECggEADy2jfxYj2Nwd6g2Z3tMUOrbRbldbuiDzp+FAeuD5Ort1\\nUIUwo/B2KI8gEfhMcG+9zyP5uJDrgE1+S40QPVy8DwchzyQHE3B4EI9q5xRGSBaR\\ncKn8UxXImcxU4h6jQ/c45N0h4OY5KILDZb5xwI/7LBGnvFLGgcPUIt0+5jTlwYsl\\n/II93af5qBLicPB34Azx8uhcAsTfXOH3SCsJWfv03MyU3j1uw7HtcvahnLDQpcrQ\\nxfrOqgxVblLfrCRMssocuqGY+5HEcpDQyoQ1M7jxvP0SfBYACNDmEJE8vBPCH8Bf\\nnaw270e4rTZR9QzLEla/q/iK4BvcurzYmRxdCOLUqQKBgQDopV7JMZ/OiudSLAtH\\nSZGGv4QXwfrpEhVLMfZ42X+xZxVppTTpf0bDTsTjoHLBAAfBGpdVOcC8b+AnSvDL\\nMSTJjABIHSS7XsBNAdDXpY6xNZBYx7qa35yws7sChy2BRZtAlR+tPwQU0vD8/ZGG\\n79BuuXd6YowH9EIpG9i8/vViLQKBgQDmXDZNgT5CQ+P3xAYvRRqxAV5rQTozEW65\\nzITPBosyifVn860zQ0encgIZEryAy3k+Ulm9MiBE9eNmOm3ZrE2WJRsHSJ8T9uOd\\nHJG2YZZUdLo4eAKygjOoj4lleLeHK86SafqtlyVMZuWJ3Qi+fWCWMdJykR1pjnja\\nDDdTwRdn5wKBgQCf0FoYo7o/zDOzwwXMZsFNa2p2V47hZMaz7RJ/WgnZ+BJBjHeY\\nnxIhQI8IP0QVSMwK3xVuOkooKEI3O8fGDXBT85SN9VcyT5iSTdkFCnnHSiBqnGmX\\n0lx1FkI1Ll8YGpTX/JjSDiPjmjRp1laN91ebeFSXAfNn02dPjg2JZytx0QKBgENu\\nWrb1TjQ3i1PLncPYhqeprunWfiLUx4S7yWSQlc6Fc8CqI9kNqLvrM5IDWgqZhTQp\\nBvvK4IdPMvGJyP4e4ddBpVfMekRt0NL8ueqZRlgSkzBUcPWwB08gNSfu3kpDGITj\\nYO3PgKuMs0RX32djbBKLIv9GW0W63sV1LfzmWOOhAoGBAK55Ga02lcZhAdMUOk7h\\nvceEXbZxrYu+0HVFjtoKB1jKJ+jfhlJGs+RJVuR7zbpbW305oYudJIlIi7BwVTAH\\nnse2qRmGjBeI6h+8Do4nWhHtv9CyYttXPT/5hS8s1WJ8CsgTLtHMv7mJTDf5WG/K\\nyqdT6w+sNbU3W9/FLsSp9E0N\\n-----END PRIVATE KEY-----\\n\",\n" +
            "  \"client_email\": \"bigquerywriter@vibrant-castle-366614.iam.gserviceaccount.com\",\n" +
            "  \"client_id\": \"103569465980283374650\",\n" +
            "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
            "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
            "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
            "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/bigquerywriter%40vibrant-castle-366614.iam.gserviceaccount.com\"\n" +
            "}\n";
    try {
      GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(str.getBytes("utf8")));
      BigQueryOptions defaultInstance = BigQueryOptions.newBuilder().setCredentials(credentials).build();
      final BigQuery bigquery = defaultInstance.getService();
      // Create rows to insert
      Map<String, Object> rowContent1 = new HashMap<>();
      rowContent1.put("id", "zzzzzzzz2");
//      rowContent1.put("name", "Gavin");
      Map<String, Object> rowContent2 = new HashMap<>();
      rowContent2.put("id", "xxxxxxxxx2");
//      rowContent2.put("name", "boy");



      InsertAllResponse response =
          bigquery.insertAll(
              InsertAllRequest.newBuilder(TableId.of(projectId,datasetName, tableName))
                      .addRow("abc", rowContent1)
                      .addRow("bcd", rowContent2)
//                  .setRows(
//                      ImmutableList.of(
//                          InsertAllRequest.RowToInsert.of("abc", rowContent1),
//                          InsertAllRequest.RowToInsert.of("bcd", rowContent2)
//                      )
//                  )
                  .build()
          );

      if (response.hasErrors()) {
        // If any of the insertions failed, this lets you inspect the errors
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
          System.out.println("Response error: \n" + entry.getValue());
        }
      }
      System.out.println("Rows successfully inserted into table without row ids");
    } catch (BigQueryException | IOException e) {
      System.out.println("Insert operation not performed \n" + e.toString());
    }
  }
}

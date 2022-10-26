package io.tapdata.bigquery.service;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.PrivateKey;
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
            "  \"private_key_id\": \"34ba4ff36e9ffb1914f7ccf36efb72cd475bf9eb\",\n" +
            "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCYbcMZXMmiwO2y\\n6eqPzGKr+CRAAOSNHyPZ0DFdbI5E4Oscg6UMCaaJLwfmD4VHMXD+1d1Y9X6LGDNT\\nEvB0s9sjJvI2kPRLm7alqUcMuJkROBT3W39cryAaKd+1Q3yEu2P7FoVSZkgAbi3J\\nNCU6jGjvzGVW3ei/aGRNVoFCCw8xnf3SoASMb+ZJvgsvGer+O/Lt1CTURjOzjDVZ\\njcdFeuiT+rpLcBZGmP4TUXNuy3ZEXKWS81gKbMBzhIhtW8sELgl9r8I+hJrnlYDk\\nD2py819Ef4bCuYJhU4rbrPpCKH5iauyty5IK1OPxZSs7vAfthLNMCPENzvo8A7Sr\\n8mM2434rAgMBAAECggEABCx5O6ANS14SBCSgjhhwGTpdr7z2hSC1qBipyV+YE62+\\n8lRueAJpo3b8teF16kmhyPCNM4rhUKi0exFZMTDdjrxZxIG6lrloSmf0sJX7ZvvM\\noytHtP98lwrPe9Shu7av2ae3tdZkIVLjAQ/i9xPyKaLEoZjI7zjKCk4UkvzfiSG6\\nBVq/8IzadG7BV2qRLBRYRFrUo06WEy3f98eUDelKyUtFKwgtzCXwNUFWocBlvGLv\\nJtHLQUYiGijwl+1KY+IlTOIllA7lwuG4R4ZvcPeSajppuDN8C3JuuRztGIvntHjI\\nklMz1J9k/dhrbUAbmPOwfzKKr2mIbjVSgO4ZUU/66QKBgQDPV6YN2paQUKwZy0XJ\\nQDVAmH1qhbe9qEaGutqsIvnD1WVE0JFIdD2bklr0m2SRpEmiIFACE7SOtzIbA1by\\nTndLkHkf8a+TKyajRI89V6U6GIAiSysUCvXESo5GyOkp/2rNro+AUUdYOtf7YgLH\\nipW+fVDWFRjUTjmVN2ho901ziQKBgQC8MxwEWNwd8FaFutz2yAKClfLwf1BHbaRi\\n2K7+GvcqOXnJ4/W5lsN5cbEbvKTO+NrM/M9oElb9jWGn2gWoQKhg6MxuM18Boh2H\\nEOIdpGXX4TKvfSmJYX80aDoiCZ4EzRGE6SelnWrKzhpO7PpBBRhDW1/jV2fHqzQW\\nbohfehrTEwKBgCohsE9eXHvkuKPhJ0QWtPt0QP/VPhneyL312BtkXAZMJXDPRMZJ\\nQH+NRMgxj0T88i1sjXVulaDuXtMYYaGJCjqjl8lC7h9khExm0Qhw99UPR3Iwfgdr\\nlrcVQ0Xk62QqT4SN9QDpAytNgbfGGbR8V6NGiZeG3+28G31TrfauUeGpAoGBALis\\nWmC1pYFHVk+xlqQejcAATjzKYUdGApnwUH8OjN0FO0nuBDDSDQx9kLJMAVkLfwDJ\\nTuirnmr9sgcYfJamo9M8fWXhyOd8YgcofQljSYB1/duQMRMa9czCPdEqqMHDTN6k\\nP4BXIPTTG6O5DLSCwFVQM56NJUwb5mfgnLc7xVi7AoGAXt7zdhrwvCRzG0WAcAz1\\niQhdK5VReebgG5fzRNse2Un2r/LUvMZ4MHE+yQgvHEoBtmO9K+WYgCQbSk0EYXxR\\nRi2CeYKTLe6iIHibn0GmlEVF3f+ELUXHT/dFy24OZ2w0T04zeKJ6cJsj/oU5K9jh\\n5a5e7bWHsVKWMklBfwkapz4=\\n-----END PRIVATE KEY-----\\n\",\n" +
            "  \"client_email\": \"asscess@vibrant-castle-366614.iam.gserviceaccount.com\",\n" +
            "  \"client_id\": \"102161354063484582804\",\n" +
            "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
            "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
            "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
            "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/asscess%40vibrant-castle-366614.iam.gserviceaccount.com\"\n" +
            "}\n";
    try {
      GoogleCredentials credentials =
              ServiceAccountCredentials.fromStream(new ByteArrayInputStream(str.getBytes("utf8")));
//              ServiceAccountCredentials.newBuilder()
//                      .setClientEmail("asscess@vibrant-castle-366614.iam.gserviceaccount.com")
//                      .setClientId("102161354063484582804")
//                      .setProjectId(projectId)
//                      .setPrivateKeyString("MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCYbcMZXMmiwO2y\\n6eqPzGKr+CRAAOSNHyPZ0DFdbI5E4Oscg6UMCaaJLwfmD4VHMXD+1d1Y9X6LGDNT\\nEvB0s9sjJvI2kPRLm7alqUcMuJkROBT3W39cryAaKd+1Q3yEu2P7FoVSZkgAbi3J\\nNCU6jGjvzGVW3ei/aGRNVoFCCw8xnf3SoASMb+ZJvgsvGer+O/Lt1CTURjOzjDVZ\\njcdFeuiT+rpLcBZGmP4TUXNuy3ZEXKWS81gKbMBzhIhtW8sELgl9r8I+hJrnlYDk\\nD2py819Ef4bCuYJhU4rbrPpCKH5iauyty5IK1OPxZSs7vAfthLNMCPENzvo8A7Sr\\n8mM2434rAgMBAAECggEABCx5O6ANS14SBCSgjhhwGTpdr7z2hSC1qBipyV+YE62+\\n8lRueAJpo3b8teF16kmhyPCNM4rhUKi0exFZMTDdjrxZxIG6lrloSmf0sJX7ZvvM\\noytHtP98lwrPe9Shu7av2ae3tdZkIVLjAQ/i9xPyKaLEoZjI7zjKCk4UkvzfiSG6\\nBVq/8IzadG7BV2qRLBRYRFrUo06WEy3f98eUDelKyUtFKwgtzCXwNUFWocBlvGLv\\nJtHLQUYiGijwl+1KY+IlTOIllA7lwuG4R4ZvcPeSajppuDN8C3JuuRztGIvntHjI\\nklMz1J9k/dhrbUAbmPOwfzKKr2mIbjVSgO4ZUU/66QKBgQDPV6YN2paQUKwZy0XJ\\nQDVAmH1qhbe9qEaGutqsIvnD1WVE0JFIdD2bklr0m2SRpEmiIFACE7SOtzIbA1by\\nTndLkHkf8a+TKyajRI89V6U6GIAiSysUCvXESo5GyOkp/2rNro+AUUdYOtf7YgLH\\nipW+fVDWFRjUTjmVN2ho901ziQKBgQC8MxwEWNwd8FaFutz2yAKClfLwf1BHbaRi\\n2K7+GvcqOXnJ4/W5lsN5cbEbvKTO+NrM/M9oElb9jWGn2gWoQKhg6MxuM18Boh2H\\nEOIdpGXX4TKvfSmJYX80aDoiCZ4EzRGE6SelnWrKzhpO7PpBBRhDW1/jV2fHqzQW\\nbohfehrTEwKBgCohsE9eXHvkuKPhJ0QWtPt0QP/VPhneyL312BtkXAZMJXDPRMZJ\\nQH+NRMgxj0T88i1sjXVulaDuXtMYYaGJCjqjl8lC7h9khExm0Qhw99UPR3Iwfgdr\\nlrcVQ0Xk62QqT4SN9QDpAytNgbfGGbR8V6NGiZeG3+28G31TrfauUeGpAoGBALis\\nWmC1pYFHVk+xlqQejcAATjzKYUdGApnwUH8OjN0FO0nuBDDSDQx9kLJMAVkLfwDJ\\nTuirnmr9sgcYfJamo9M8fWXhyOd8YgcofQljSYB1/duQMRMa9czCPdEqqMHDTN6k\\nP4BXIPTTG6O5DLSCwFVQM56NJUwb5mfgnLc7xVi7AoGAXt7zdhrwvCRzG0WAcAz1\\niQhdK5VReebgG5fzRNse2Un2r/LUvMZ4MHE+yQgvHEoBtmO9K+WYgCQbSk0EYXxR\\nRi2CeYKTLe6iIHibn0GmlEVF3f+ELUXHT/dFy24OZ2w0T04zeKJ6cJsj/oU5K9jh\\n5a5e7bWHsVKWMklBfwkapz4=")
//                      .setPrivateKeyId("34ba4ff36e9ffb1914f7ccf36efb72cd475bf9eb")
//                      .build();
      BigQueryOptions defaultInstance = BigQueryOptions.newBuilder().setProjectId(projectId).setCredentials(credentials).build();
      final BigQuery bigquery = defaultInstance.getService();
      // Create rows to insert
      Map<String, Object> rowContent1 = new HashMap<>();
      rowContent1.put("id", "111");
      rowContent1.put("name", "Gavin");
      Map<String, Object> rowContent2 = new HashMap<>();
      rowContent2.put("id", "112");
      rowContent2.put("name", "boy");
      InsertAllResponse response =
          bigquery.insertAll(
              InsertAllRequest.newBuilder(TableId.of(projectId,datasetName, tableName))
                  .setRows(
                      ImmutableList.of(
                          InsertAllRequest.RowToInsert.of(rowContent1),
                          InsertAllRequest.RowToInsert.of(rowContent2)))
                  .build());

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
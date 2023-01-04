package io.tapdata.bigquery.service.stream;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializtionError;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.cloud.bigquery.storage.v1.stub.BigQueryWriteStubSettings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.grpc.Status;
import io.grpc.Status.Code;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import javax.annotation.concurrent.GuardedBy;

import io.tapdata.entity.error.CoreException;
import org.json.JSONArray;
import org.json.JSONObject;


public class WriteToDefaultStream {
  DataWriter writer ;
  public static WriteToDefaultStream init(){
//    TableName parentTable = TableName.of(projectId, datasetName, tableName);
//    // One time initialization for the worker.
//    writer.initialize(parentTable);
//
//    writer = new DataWriter();
    return new WriteToDefaultStream();
  }
  public void writeToDefaultStream()
      throws DescriptorValidationException, InterruptedException, IOException {
    cleanBody();
    //verifyExpectedRowCount(parentTable, 12);
    //System.out.println("Appended records successfully.");
  }

  public static void writeToDefaultStream(String projectId,String datasetName,String tableName)
      throws DescriptorValidationException, InterruptedException, IOException {
    TableName parentTable = TableName.of(projectId, datasetName, tableName);

    DataWriter writer = new DataWriter();
    // One time initialization for the worker.
    writer.initialize(parentTable);

    long start = System.currentTimeMillis();
    // Write two batches of fake data to the stream, each with 10 JSON records.  Data may be
    // batched up to the maximum request size:
    // https://cloud.google.com/bigquery/quotas#write-api-limits
    for (int i = 0; i < 2; i++) {
      // Create a JSON object that is compatible with the table schema.
      JSONArray jsonArr = new JSONArray();
      for (int j = 0; j < 10; j++) {
        JSONObject record = new JSONObject();
//        StringBuilder sbSuffix = new StringBuilder();
//        for (int k = 0; k < j; k++) {
//          sbSuffix.append(k);
//        }
        record.put("_id", "20230101"+j);
        record.put("id", "no1");
        record.put("name", "hello");
        record.put("type", 11.00);
        jsonArr.put(record);
      }
      writer.append(new AppendContext(jsonArr, 0));
    }

    System.out.println("time(s): "+ (System.currentTimeMillis() - start) );
    // Final cleanup for the stream during worker teardown.
    writer.cleanup();
    verifyExpectedRowCount(parentTable, 12);
    System.out.println("Appended records successfully.");
  }

  private static void verifyExpectedRowCount(TableName parentTable, int expectedRowCount)
      throws InterruptedException {
    String queryRowCount =
        "SELECT COUNT(*) FROM `"
            + parentTable.getProject()
            + "."
            + parentTable.getDataset()
            + "."
            + parentTable.getTable()
            + "`";
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryRowCount).build();
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    TableResult results = bigquery.query(queryConfig);
    int countRowsActual =
        Integer.parseInt(results.getValues().iterator().next().get("f0_").getStringValue());
    if (countRowsActual != expectedRowCount) {
      throw new RuntimeException(
          "Unexpected row count. Expected: " + expectedRowCount + ". Actual: " + countRowsActual);
    }
  }


  private static class AppendContext {

    JSONArray data;
    int retryCount = 0;

    AppendContext(JSONArray data, int retryCount) {
      this.data = data;
      this.retryCount = retryCount;
    }
  }

  public static String credential = "{\n" +
          "  \"type\": \"service_account\",\n" +
          "  \"project_id\": \"vibrant-castle-366614\",\n" +
          "  \"private_key_id\": \"b3c32fdb4ce834da98f1b139736fee147c88909e\",\n" +
          "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCZva4Wo37cmlUW\\nxgtL3v0/inpBaHoYdF3JqR0iK84zKuDt5VqP8BIyGufFYYkpxdaR90PNNnZgV2Ko\\nhXU9SjQ7Z7inxZCTS8KXzVnX/3fGNsdxeBW7xrRIKvdm7Jf//4SGkX/0becyof+X\\nsMJHuEkMkcc3Y3VMewYZ42G/gV8KtC0SwR0RagAfRxP6CC/flPX3HT5ffs5Q4G5M\\n66kq0KLoLhcpfAQElQWrmnKanXPaW2wsh/V5EAwGG1PmcU5zDZ5Fg2DPsQ3PjhOX\\n0Xrzgw24ukGTKv+NS0jOdaWQuWYW/0ALubJSshBIUuetH7DFSocTPvc5J3G0GqXP\\nX3oVIrClAgMBAAECggEADXXoCh9iehohHQ9V6dyqO6f6MEPffMijdYaTAGzpbt1w\\nOCP+m9+fGDf21vdFNR0XPkxx6UO9dY3xG2Qj8avPiuv35OiNUfguH3BhT2IUsIwX\\nRj4HWRt6qV7prl9Ep6tNhSK0G0iMF4jLghJ90B24d5tD3/ubR4j17cpUwpmnIp6l\\nHjuZAe0Q1sMKnSeBZ31yxdgZaHlhXAj3WBGLk9bbdNwY+fXMLZA3Wut9IRGbXyzT\\nbcToqZlJojOeNPo4cHjBcg6PjhrGz4EY0bCb6z7idqTaSeawDbGSEueQsygtcoIL\\nlGJV0sCAigstZpSbXy89OpvDj5HztFk16Wc1Dt5+eQKBgQDHCwxP2dz1OsQ8/T6V\\nErvPxK46zVUP7F+NjsYH/yT7sJrbIwGvZoShlviy9WnocmzZIsrSe4HVlfmE9yCI\\n8OZnPxYzTlK7aAH+vhWPp7oQqasAsAaeYTI237oHw9lnSOmaFUtl8qwfnSsWiF0g\\nO9nHkdLYK0pN+bpf+7U5O0vRHwKBgQDFvAHuEyRXjHJcMtr6xFVYYCtpRGiod9oL\\ny2cM/M43bMajWcirBTIrAxrrnFWzKuPXAShfG85+BeSUGxu0s80B5a3MIEWQfu3z\\nthFUU2xKta6TevetclamHzNhXAysJdHrRmJr6fUyUTmCXVTQb0TUH9mZPUQ7cyFD\\n1h0SRQYxuwKBgDTWLPWBetMqP2+FNjiyWWLE7g8z9JGeiJr2PIFg7HtXnTPwrgDW\\nsPyILAqtdOi8f0KApuCK4qNFBZCTXXKcqDzeFVGXSATxjh4GbYjN2GmV8IvlLkya\\nto60gxiOl8aAJ2q8nmA4tBJMUWTQ3A+zc5MzlYnGrBnY4e2azrebkvu3AoGAD8w5\\niz/UQ3phGKSngil1eB4W2c4xXmRU82RI02zPPPZf2GUv9xnvLCiPWguffTUMBv18\\nsDyUftURshOIXyOOWXx0Kj7Zz/WUJUiCke4oVL+3Nuk4KI9eBN+xRzIHgSl0YAu7\\niUuj32VF5vh18kExipEQ3YFbljRYkAbnQ7JoEEkCgYAiOKYIbfRALl2GJdmve+cI\\n8eHFjT/PKUg99bTagRW5Z/sN4jU0j6TSzI/xEjPNW7WP8g9xE6SHo3WHAn9aVqOW\\nZB0qr962YshL4n0NtDCO73UX6EGdIEii+9IyEHEwbbb3DYp5SM0WaLa52ZyT5E0X\\n6dEb/BwoMUwF+ZwXSwWzQg==\\n-----END PRIVATE KEY-----\\n\",\n" +
          "  \"client_email\": \"acountbygavin@vibrant-castle-366614.iam.gserviceaccount.com\",\n" +
          "  \"client_id\": \"111681922313258447427\",\n" +
          "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
          "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
          "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
          "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/acountbygavin%40vibrant-castle-366614.iam.gserviceaccount.com\"\n" +
          "}";

  public static String projectId = "vibrant-castle-366614";

  private static class DataWriter {
    private static final int MAX_RETRY_COUNT = 3;
    private static final ImmutableList<Code> RETRIABLE_ERROR_CODES =
        ImmutableList.of(
            Code.INTERNAL,
            Code.ABORTED,
            Code.CANCELLED,
            Code.FAILED_PRECONDITION,
            Code.DEADLINE_EXCEEDED,
            Code.UNAVAILABLE);
    // Track the number of in-flight requests to wait for all responses before shutting down.
    private final Phaser inflightRequestCount = new Phaser(1);
    private final Object lock = new Object();
    private JsonStreamWriter streamWriter;
    @GuardedBy("lock")
    private RuntimeException error = null;
    public void initialize(TableName parentTable)
        throws DescriptorValidationException, IOException, InterruptedException {
      // Use the JSON stream writer to send records in JSON format. Specify the table name to write
      // to the default stream.
      // For more information about JsonStreamWriter, see:
      // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html
      //GcpStorageProperties gcpStorageProperties

//      GoogleCredentials credentials = ServiceAccountCredentials
//              .fromStream(new ByteArrayInputStream(credential.getBytes(StandardCharsets.UTF_8)));
//      BigQueryWriteStubSettings build = BigQueryWriteStubSettings.newBuilder()
//              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
//              .setQuotaProjectId(projectId)
//              .build();

      GoogleCredentials googleCredentials = getGoogleCredentials();
      BigQueryWriteSettings settings =
              BigQueryWriteSettings.newBuilder().setCredentialsProvider(() -> googleCredentials).build();

      BigQueryWriteClient bigQueryWriteClient = BigQueryWriteClient
              .create(settings);
      JsonStreamWriter.Builder builder = JsonStreamWriter.newBuilder(parentTable.toString(), bigQueryWriteClient);
      streamWriter = builder.build();
    }

    private GoogleCredentials getGoogleCredentials() {
      try {
        return GoogleCredentials
                .fromStream(new ByteArrayInputStream(credential.getBytes(StandardCharsets.UTF_8)));
      } catch (IOException e) {
        throw new CoreException("Big query connector direct fail exception, connector not handle this exception");
      }
    }

    public void append(AppendContext appendContext)
        throws DescriptorValidationException, IOException {
      synchronized (this.lock) {
        // If earlier appends have failed, we need to reset before continuing.
        if (this.error != null) {
          throw this.error;
        }
      }
      // Append asynchronously for increased throughput.
      ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
      ApiFutures.addCallback(
          future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());
      // Increase the count of in-flight requests.
      inflightRequestCount.register();
    }

    public void cleanup() {
      // Wait for all in-flight requests to complete.
      inflightRequestCount.arriveAndAwaitAdvance();
      // Close the connection to the server.
      streamWriter.close();
      // Verify that no error occurred in the stream.
      synchronized (this.lock) {
        if (this.error != null) {
          throw this.error;
        }
      }
    }

    static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {
      private final DataWriter parent;
      private final AppendContext appendContext;
      // Prepare a thread pool
      static ExecutorService pool = Executors.newFixedThreadPool(50);
      public AppendCompleteCallback(DataWriter parent, AppendContext appendContext) {
        this.parent = parent;
        this.appendContext = appendContext;
      }
      public void onSuccess(AppendRowsResponse response) {
        System.out.format("Append success%n");
        done();
      }
      public void onFailure(Throwable throwable) {
        // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
        // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
        // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
        Status status = Status.fromThrowable(throwable);
        if (appendContext.retryCount < MAX_RETRY_COUNT
            && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
          appendContext.retryCount++;
          // Use a separate thread to avoid potentially blocking while we are in a callback.
          pool.submit(
              () -> {
                try {
                  // Since default stream appends are not ordered, we can simply retry the
                  // appends.
                  // Retrying with exclusive streams requires more careful consideration.
                  this.parent.append(appendContext);
                } catch (Exception e) {
                  // Fall through to return error.
                  System.out.format("Failed to retry append: %s%n", e);
                }
              });
          // Mark the existing attempt as done since it's being retried.
          done();
          return;
        }
        if (throwable instanceof AppendSerializtionError) {
          AppendSerializtionError ase = (AppendSerializtionError) throwable;
          Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
          if (rowIndexToErrorMessage.size() > 0) {
            // Omit the faulty rows
            JSONArray dataNew = new JSONArray();
            for (int i = 0; i < appendContext.data.length(); i++) {
              if (!rowIndexToErrorMessage.containsKey(i)) {
                dataNew.put(appendContext.data.get(i));
              } else {
                // process faulty rows by placing them on a dead-letter-queue, for instance
              }
            }
            // Mark the existing attempt as done since we got a response for it
            done();
            // Retry the remaining valid rows, but using a separate thread to
            // avoid potentially blocking while we are in a callback.
            if (dataNew.length() > 0) {
              pool.submit(
                  () -> {
                    try {
                      this.parent.append(new AppendContext(dataNew, 0));
                    } catch (Exception e2) {
                      System.out.format("Failed to retry append with filtered rows: %s%n", e2);
                    }
                  });
            }
            return;
          }
        }
        synchronized (this.parent.lock) {
          if (this.parent.error == null) {
            StorageException storageException = Exceptions.toStorageException(throwable);
            this.parent.error =
                (storageException != null) ? storageException : new RuntimeException(throwable);
          }
        }
        System.out.format("Error that arrived: %s%n", throwable);
        done();
      }
      private void done() {
        // Reduce the count of in-flight requests.
        this.parent.inflightRequestCount.arriveAndDeregister();
      }
    }
  }

  public void builderBody(List<Map<String,Object>> recordMapArr) throws IOException, DescriptorValidationException {
    cleanBody();
    JSONArray jsonArr = new JSONArray();
    recordMapArr.stream().filter(Objects::nonNull).forEach(jsonArr::put);
    writer.append(new AppendContext(jsonArr, 0));
  }
  public void appendBody(Map<String,Object> recordMap) throws IOException, DescriptorValidationException {
    writer.append(new AppendContext(new JSONArray().put(recordMap), 0));
  }
  public void cleanBody(){
    writer.cleanup();
  }
}
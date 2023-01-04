package io.tapdata.bigquery.service.stream;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.cloud.bigquery.storage.v1.stub.BigQueryWriteStubSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import javax.annotation.concurrent.GuardedBy;

import io.tapdata.bigquery.service.stream.handle.BigQueryStream;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import org.json.JSONArray;
import org.json.JSONObject;

public class WriteCommittedStream {
  public static final String TAG = WriteCommittedStream.class.getSimpleName();
  String credentialsJson ;
  private String projectId;
  private String dataSet;
  private String tableName;

  BigQueryWriteClient client;
  DataWriter writer;
  public String projectId(){
    return this.projectId;
  }
  public String dataSet(){
    return this.dataSet;
  }
  public String tableName(){
    return this.tableName;
  }
  public String credentialsJson(){
    return this.credentialsJson;
  }
  public WriteCommittedStream projectId(String projectId){
    this.projectId = projectId;
    return this;
  }
  public WriteCommittedStream dataSet(String dataSet){
    this.dataSet = dataSet;
    return this;
  }
  public WriteCommittedStream tableName(String tableName) throws DescriptorValidationException, InterruptedException, IOException {
    this.tableName = tableName;
    return this;
  }
  public WriteCommittedStream credentialsJson(String credentialsJson){
    this.credentialsJson = credentialsJson;
    return this;
  }
  public static WriteCommittedStream writer(String projectId,String dataSet,String tableName,String credentialsJson) throws DescriptorValidationException, InterruptedException, IOException {
    WriteCommittedStream writeCommittedStream = new WriteCommittedStream()
            .projectId(projectId)
            .dataSet(dataSet)
            .tableName(tableName)
            .credentialsJson(credentialsJson);
    return writeCommittedStream.init();
  }

  private WriteCommittedStream(){

  }
  private WriteCommittedStream init() throws IOException, DescriptorValidationException, InterruptedException {
    GoogleCredentials googleCredentials = getGoogleCredentials(credentialsJson);
    BigQueryWriteSettings settings =
            BigQueryWriteSettings.newBuilder().setCredentialsProvider(() -> googleCredentials).build();
    client = BigQueryWriteClient.create(settings);
    TableName parentTable = TableName.of(projectId, dataSet, tableName);
    // One time initialization.
    writer = new DataWriter();
    writer.initialize(parentTable, client, credentialsJson);
    return this;
  }

  private GoogleCredentials getGoogleCredentials(String credentialsJson) {
    try {
      return GoogleCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new CoreException("Big query connector direct fail exception, connector not handle this exception");
    }
  }
  public void writeCommittedStream() throws DescriptorValidationException, IOException, InterruptedException {
    try {
      // Write two batches of fake data to the stream, each with 10 JSON records.  Data may be
      // batched up to the maximum request size:
      // https://cloud.google.com/bigquery/quotas#write-api-limits
      long start = System.currentTimeMillis();
      long offset = 0;
      for (int i = 0; i < 5000; i++) {
        // Create a JSON object that is compatible with the table schema.
        JSONArray jsonArr = new JSONArray();
        for (int j = 0; j < 10; j++) {
          JSONObject record = new JSONObject();
          record.put("_id", "20230104"+i+j);
          record.put("id", "20230104"+i+j);
          record.put("name", "SSS");
          record.put("type", 12.6);
          jsonArr.put(record);
        }
        writer.append(jsonArr, offset);
        offset += jsonArr.length();
      }
      System.out.println(System.currentTimeMillis() - start);
    } catch (ExecutionException e) {
      e.printStackTrace();
      // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
      // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information, see:
      // https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
      //System.out.println("Failed to append records. \n" + e);
    }

    // Final cleanup for the stream.
    writer.cleanup(client);
    //System.out.println("Appended records successfully.");
  }

  public void append(List<Map<String,Object>> record) throws IOException, DescriptorValidationException, InterruptedException {
    if (Objects.isNull(record) || record.isEmpty()) return;
    long offset = 0;
    try {
      JSONArray jsonArr = new JSONArray();
      for (Map<String, Object> map : record) {
        if (Objects.isNull(map)) continue;
        JSONObject jsonObject = new JSONObject();
        map.forEach(jsonObject::put);
        jsonArr.put(jsonObject);
        writer.append(jsonArr, offset);
        offset += jsonArr.length();
      }
    }catch(ExecutionException e){
      //TapLogger.info(TAG,"Error to use stream api write records: "+e.getMessage());
    }
    writer.cleanup(client);
  }

  // A simple wrapper object showing how the stateful stream writer should be used.
  private static class DataWriter {

    private JsonStreamWriter streamWriter;
    // Track the number of in-flight requests to wait for all responses before shutting down.
    private final Phaser inflightRequestCount = new Phaser(1);

    private final Object lock = new Object();

    @GuardedBy("lock")
    private RuntimeException error = null;

    void initialize(TableName parentTable, BigQueryWriteClient client,String credentialsJson)
        throws IOException, DescriptorValidationException, InterruptedException {
      // Initialize a write stream for the specified table.
      // For more information on WriteStream.Type, see:
      // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/WriteStream.Type.html
      WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();

      CreateWriteStreamRequest createWriteStreamRequest =
          CreateWriteStreamRequest.newBuilder()
              .setParent(parentTable.toString())
              .setWriteStream(stream)
              .build();
      WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

      // Use the JSON stream writer to send records in JSON format.
      // For more information about JsonStreamWriter, see:
      // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html

      GoogleCredentials credentials =
              ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8)));
      streamWriter = JsonStreamWriter
                  .newBuilder(writeStream.getName(), writeStream.getTableSchema())
                  .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                  .build();
    }

    public void append(JSONArray data, long offset)
        throws DescriptorValidationException, IOException, ExecutionException {
      synchronized (this.lock) {
        // If earlier appends have failed, we need to reset before continuing.
        if (this.error != null) {
          throw this.error;
        }
      }
      // Append asynchronously for increased throughput.
      ApiFuture<AppendRowsResponse> future = streamWriter.append(data, offset);
      ApiFutures.addCallback(
          future, new DataWriter.AppendCompleteCallback(this), MoreExecutors.directExecutor());
      // Increase the count of in-flight requests.
      inflightRequestCount.register();
    }

    public void cleanup(BigQueryWriteClient client) {
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

      // Finalize the stream.
      FinalizeWriteStreamResponse finalizeResponse =
          client.finalizeWriteStream(streamWriter.getStreamName());
    }

    public String getStreamName() {
      return streamWriter.getStreamName();
    }

    static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

      private final DataWriter parent;

      public AppendCompleteCallback(DataWriter parent) {
        this.parent = parent;
      }

      public void onSuccess(AppendRowsResponse response) {
        TapLogger.info(TAG,String.format("Append %d success ", response.getAppendResult().getOffset().getValue()));
        done();
      }

      public void onFailure(Throwable throwable) {
        synchronized (this.parent.lock) {
          if (this.parent.error == null) {
            StorageException storageException = Exceptions.toStorageException(throwable);
            this.parent.error =
                (storageException != null) ? storageException : new RuntimeException(throwable);
          }
        }
        TapLogger.error(TAG,"Error: "+throwable.getMessage());
        done();
        throw new CoreException("Error: "+throwable.getMessage());
      }

      private void done() {
        // Reduce the count of in-flight requests.
        this.parent.inflightRequestCount.arriveAndDeregister();
      }
    }
  }
}
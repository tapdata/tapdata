//package io.tapdata.bigquery.service;
//
//import com.google.cloud.bigquery.connection.v1.CloudSqlCredential;
//import com.google.cloud.bigquery.connection.v1.CloudSqlProperties;
//import com.google.cloud.bigquery.connection.v1.Connection;
//import com.google.cloud.bigquery.connection.v1.CreateConnectionRequest;
//import com.google.cloud.bigquery.connection.v1.LocationName;
//import com.google.cloud.bigqueryconnection.v1.ConnectionServiceClient;
//import java.io.IOException;
//
//// Sample to create a connection with cloud MySql database
//public class CreateConnection {
//
//  public static void main(String[] args) throws IOException {
//    // TODO(developer): Replace these variables before running the sample.
//    String projectId = "MY_PROJECT_ID";
//    String location = "MY_LOCATION";
//    String connectionId = "MY_CONNECTION_ID";
//    String database = "MY_DATABASE";
//    String instance = "MY_INSTANCE";
//    String instanceLocation = "MY_INSTANCE_LOCATION";
//    String username = "MY_USERNAME";
//    String password = "MY_PASSWORD";
//    String instanceId = String.format("%s:%s:%s", projectId, instanceLocation, instance);
//    CloudSqlCredential cloudSqlCredential =
//        CloudSqlCredential.newBuilder().setUsername(username).setPassword(password).build();
//    CloudSqlProperties cloudSqlProperties =
//        CloudSqlProperties.newBuilder()
//            .setType(CloudSqlProperties.DatabaseType.MYSQL)
//            .setDatabase(database)
//            .setInstanceId(instanceId)
//            .setCredential(cloudSqlCredential)
//            .build();
//    Connection connection = Connection.newBuilder().setCloudSql(cloudSqlProperties).build();
//    createConnection(projectId, location, connectionId, connection);
//  }
//
//  public static void createConnection(
//      String projectId, String location, String connectionId, Connection connection)
//      throws IOException {
//    try (ConnectionServiceClient client = ConnectionServiceClient.create()) {
//      LocationName parent = LocationName.of(projectId, location);
//      CreateConnectionRequest request =
//          CreateConnectionRequest.newBuilder()
//              .setParent(parent.toString())
//              .setConnection(connection)
//              .setConnectionId(connectionId)
//              .build();
//      Connection response = client.createConnection(request);
//      System.out.println("Connection created successfully :" + response.getName());
//    }
//  }
//}
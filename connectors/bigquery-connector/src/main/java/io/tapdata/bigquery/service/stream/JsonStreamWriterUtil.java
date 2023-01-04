package io.tapdata.bigquery.service.stream;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.stub.BigQueryWriteStubSettings;
import com.google.protobuf.Descriptors;
import io.tapdata.entity.error.CoreException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class JsonStreamWriterUtil {
    public static JsonStreamWriterUtil create(String projectId,String dataSet){
        if (Objects.isNull(util)){
            synchronized (JsonStreamWriterUtil.class){
                util = new JsonStreamWriterUtil();
            }
        }
        return util.projectId(projectId).dataSet(dataSet);
    }
    private JsonStreamWriterUtil(){

    }
    private String projectId;
    public String projectId(){return this.projectId;}
    public JsonStreamWriterUtil projectId(String projectId){
        this.projectId = projectId;
        return this;
    }
    private String dataSet;
    public String dataSet(){return this.dataSet;}
    public JsonStreamWriterUtil dataSet(String dataSet){
        this.dataSet = dataSet;
        return this;
    }
    private static JsonStreamWriterUtil util;
    private static final Map<String ,StreamWriter> jsonStreamWriterMap = new HashMap<>();
    public StreamWriter getWriteStreamMap(String tableName){
        return Optional.ofNullable(jsonStreamWriterMap.get(tableName))
                .orElse(util.createWriteStream(tableName));
    }

    String credentialsJson = "{\n" +
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
    public StreamWriter createWriteStream(String table) {
        if (Objects.isNull(util.projectId)) throw new CoreException("Project id must not be null or not be empty.");
        if (Objects.isNull(util.dataSet)) throw new CoreException("DataSet must not be null or not be empty.");
        try {
            TableName tableName = TableName.of(util.projectId(),util.dataSet(), table);

            GoogleCredentials googleCredentials = getGoogleCredentials();
            BigQueryWriteSettings settings =
                    BigQueryWriteSettings.newBuilder().setCredentialsProvider(() -> googleCredentials).build();

            BigQueryWriteClient client = BigQueryWriteClient
                    .create(settings);

            CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                    .setParent(tableName.toString())
                    .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build())
                    .build();

            GoogleCredentials credentials =
                    ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8)));
            BigQueryWriteStubSettings build = BigQueryWriteStubSettings.newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
            BigQueryWriteClient bigQueryWriteClient = BigQueryWriteClient.create(BigQueryWriteSettings.create(build));

            WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
            WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

            Field.Builder name = Field.of("name", LegacySQLTypeName.STRING).toBuilder();
            name.setMode(Field.Mode.NULLABLE);

            Field.Builder _id = Field.of("_id", LegacySQLTypeName.STRING).toBuilder();
            _id.setMode(Field.Mode.NULLABLE);

            Field.Builder id = Field.of("id", LegacySQLTypeName.STRING).toBuilder();
            id.setMode(Field.Mode.NULLABLE);

            Field.Builder type = Field.of("type", LegacySQLTypeName.BIGNUMERIC).toBuilder();
            type.setMode(Field.Mode.NULLABLE);



            FieldList fieldsList = FieldList.of(
                    name.build(),
                    _id.build(),
                    id.build(),
                    type.build()
            );
            TableSchema buildTableSchema = TableSchema.newBuilder().addAllFields(toTableFieldsSchema(fieldsList)).build();
            Descriptors.Descriptor descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(buildTableSchema);
            ProtoSchema convert = ProtoSchemaConverter.convert(descriptor);

            StreamWriter streamWriter = StreamWriter
                    .newBuilder(writeStream.getName(), client)
                    .setWriterSchema(convert)
                    .build();

//            JsonStreamWriter.Builder builder = JsonStreamWriter
//                    .newBuilder(writeStream.getName(), client);
//            JsonStreamWriter jsonStreamWriter = builder.build();

            jsonStreamWriterMap.put(table,streamWriter);
            return streamWriter;
        }catch (Exception e){
            throw new CoreException(e.getMessage());
        }
    }

    private List<TableFieldSchema> toTableFieldsSchema(FieldList fieldsList) {
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>(fieldsList.size());
        for (Field field : fieldsList) {
            TableFieldSchema.Builder tableField = TableFieldSchema.newBuilder().setName(field.getName());
            if (field.getType() == LegacySQLTypeName.FLOAT) {
                tableField.setType(TableFieldSchema.Type.DOUBLE);
            } else {
                tableField.setType(TableFieldSchema.Type.valueOf(field.getType().getStandardType().name()));
            }
            tableField.setMode(TableFieldSchema.Mode.valueOf(field.getMode().name()));
            Optional.ofNullable(field.getDescription()).ifPresent(
                    description -> tableField.setDescription(description));
            if (field.getType() == LegacySQLTypeName.RECORD) {
                FieldList subFields = field.getSubFields();
                for (TableFieldSchema tableFieldSchema : toTableFieldsSchema(subFields)) {
                    tableField.addFields(tableFieldSchema);
                }
            }
            tableFieldSchemas.add(tableField.build());
        }
        return tableFieldSchemas;
    }



    public StreamWriter createWriteStream(String projectId,String dataSet,String table){
        return create(projectId, dataSet).createWriteStream(table);
    }
    private GoogleCredentials getGoogleCredentials() {
        try {
            return GoogleCredentials
                    .fromStream(new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8)));
        } catch (IOException e) {
            throw new CoreException("Big query connector direct fail exception, connector not handle this exception");
        }
    }
}

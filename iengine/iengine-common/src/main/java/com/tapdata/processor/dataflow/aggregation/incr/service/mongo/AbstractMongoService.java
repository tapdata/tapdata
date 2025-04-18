package com.tapdata.processor.dataflow.aggregation.incr.service.mongo;


import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.aggregation.incr.service.LifeCycleService;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;

abstract public class AbstractMongoService implements LifeCycleService {

	protected final MongoClient mongoClient;
	protected final MongoDatabase database;
	protected final Stage stage;

	public AbstractMongoService(Stage stage, Connections connections) {
		this.stage = stage;
		String databaseName;
		if (StringUtils.isEmpty(connections.getDatabase_uri())) {
			ServerAddress serverAddress = new ServerAddress(connections.getDatabase_host(), connections.getDatabase_port());
			MongoCredential credential = MongoCredential.createCredential(connections.getDatabase_username(), connections.getDatabase_name(), connections.getDatabase_password().toCharArray());
			MongoClientSettings options = MongoClientSettings.builder()
					.applyToSslSettings(builder -> builder.enabled(connections.getSsl()))
					.codecRegistry(MongodbUtil.getForJavaCoedcRegistry())
					.applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(serverAddress)))
					.credential(credential)
					.build();
			mongoClient = new MongoClientImpl(options, MongoDriverInformation.builder().build());
			databaseName = connections.getDatabase_name();
		} else {
			final MongoClientSettings.Builder builder = MongoClientSettings.builder().codecRegistry(MongodbUtil.getForJavaCoedcRegistry());
			ConnectionString mongoClientURI = new ConnectionString(connections.getDatabase_uri());
			mongoClient = new MongoClientImpl(builder.applyConnectionString(mongoClientURI).build(), MongoDriverInformation.builder().build());
			databaseName = mongoClientURI.getDatabase();
		}
		this.database = mongoClient.getDatabase(databaseName);
	}

	@Override
	public void destroy() {
		if (mongoClient != null) {
			mongoClient.close();
		}
	}
}

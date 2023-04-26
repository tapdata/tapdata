package com.tapdata.processor.dataflow.aggregation.incr.service.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.aggregation.incr.service.LifeCycleService;
import org.apache.commons.lang3.StringUtils;

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
			MongoClientOptions options = MongoClientOptions.builder().sslEnabled(connections.getSsl()).codecRegistry(MongodbUtil.getForJavaCoedcRegistry()).build();
			mongoClient = new MongoClient(serverAddress, credential, options);
			databaseName = connections.getDatabase_name();
		} else {
			final MongoClientOptions.Builder builder = MongoClientOptions.builder().codecRegistry(MongodbUtil.getForJavaCoedcRegistry());
			MongoClientURI mongoClientURI = new MongoClientURI(connections.getDatabase_uri(), builder);
			mongoClient = new MongoClient(mongoClientURI);
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

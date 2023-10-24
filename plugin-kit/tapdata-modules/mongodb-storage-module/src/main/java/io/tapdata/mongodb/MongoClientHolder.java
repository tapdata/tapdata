package io.tapdata.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;

public class MongoClientHolder {
	MongoClient mongoClient;
	public MongoClientHolder mongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
		return this;
	}
	ConnectionString connectionString;
	public MongoClientHolder connectionString(ConnectionString connectionString) {
		this.connectionString = connectionString;
		return this;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public ConnectionString getConnectionString() {
		return connectionString;
	}

	public void setConnectionString(ConnectionString connectionString) {
		this.connectionString = connectionString;
	}
}

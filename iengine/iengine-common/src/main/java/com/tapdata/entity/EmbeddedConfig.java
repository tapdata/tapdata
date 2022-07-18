package com.tapdata.entity;

/**
 * @author huangjq
 * @ClassName; embedded2Mongo
 * @Description; TODO
 * @date 2017/5/17 14;08
 * @since 1.0
 */
public class EmbeddedConfig {

	/**
	 *
	 */
	private String database_type;

	/**
	 *
	 */
	private String database_host;

	/**
	 *
	 */
	private String database_name;

	/**
	 *
	 */
	private String database_username;


	/**
	 *
	 */
	private Integer database_port;

	/**
	 *
	 */
	private String database_password;

	/**
	 *
	 */
	private String mongo_uri;

	/**
	 *
	 */
	private String mongo_database;

	public String getDatabase_type() {
		return database_type;
	}

	public void setDatabase_type(String database_type) {
		this.database_type = database_type;
	}

	public String getDatabase_host() {
		return database_host;
	}

	public void setDatabase_host(String database_host) {
		this.database_host = database_host;
	}

	public String getDatabase_name() {
		return database_name;
	}

	public void setDatabase_name(String database_name) {
		this.database_name = database_name;
	}

	public String getDatabase_username() {
		return database_username;
	}

	public void setDatabase_username(String database_username) {
		this.database_username = database_username;
	}

	public Integer getDatabase_port() {
		return database_port;
	}

	public void setDatabase_port(Integer database_port) {
		this.database_port = database_port;
	}

	public String getDatabase_password() {
		return database_password;
	}

	public void setDatabase_password(String database_password) {
		this.database_password = database_password;
	}

	public String getMongo_uri() {
		return mongo_uri;
	}

	public void setMongo_uri(String mongo_uri) {
		this.mongo_uri = mongo_uri;
	}

	public String getMongo_database() {
		return mongo_database;
	}

	public void setMongo_database(String mongo_database) {
		this.mongo_database = mongo_database;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("EmbeddedConfig{");
		sb.append("database_type='").append(database_type).append('\'');
		sb.append(", database_host='").append(database_host).append('\'');
		sb.append(", database_name='").append(database_name).append('\'');
		sb.append(", database_username='").append(database_username).append('\'');
		sb.append(", database_port='").append(database_port).append('\'');
		sb.append(", database_password='").append(database_password).append('\'');
		sb.append(", mongo_uri='").append(mongo_uri).append('\'');
		sb.append(", mongo_database='").append(mongo_database).append('\'');
		sb.append('}');
		return sb.toString();
	}
}

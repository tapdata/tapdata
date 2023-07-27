package io.tapdata.mongodb.entity;

import com.mongodb.ConnectionString;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

public class MongodbConfig extends CommonDbConfig {

	private boolean isUri = true;

	private String uri;

	private String host;

	private String database;

	private String user;

	private String password;

	private String additionalString;

	private boolean ssl;

	private String sslCA;

	private String sslKey;

	private String sslPass;

	private boolean sslValidate;

	private boolean checkServerIdentity;

	private String insertDmlPolicy;

	private String updateDmlPolicy;

	private boolean enableSaveDeleteData;

	private boolean enableFillingModifiedData;

	private boolean noCursorTimeout;

	private boolean syncIndex;

	public boolean isSyncIndex() {
		return syncIndex;
	}

	public void setSyncIndex(boolean syncIndex) {
		this.syncIndex = syncIndex;
	}

//	public static MongodbConfig load(String jsonFile) throws IOException {
//		ObjectMapper mapper = new ObjectMapper(new JsonFactory());
//		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//		return mapper.readValue(new File(jsonFile), MongodbConfig.class);
//	}
//
//	public static MongodbConfig load(Map<String, Object> map) throws IOException {
//		ObjectMapper mapper = new ObjectMapper();
//		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//		return mapper.readValue((new ObjectMapper()).writeValueAsString(map), MongodbConfig.class);
//	}

	public String getUri() {
		if (isUri) {
			return uri;
		} else {
			StringBuilder sb = new StringBuilder("mongodb://");
			if (EmptyKit.isNotEmpty(this.getUser()) && EmptyKit.isNotEmpty(this.getPassword())) {
				String encodeUsername = null;
				String encodePassword = null;
				try {
					encodeUsername = URLEncoder.encode(this.getUser(), "UTF-8");
					encodePassword = URLEncoder.encode(this.getPassword(), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(String.format("Encoding mongodb username/password failed %s", e.getMessage()), e);
				}
				sb.append(encodeUsername).append(":").append(encodePassword).append("@");
			}
			sb.append(this.getHost().trim());
			sb.append("/").append(this.getDatabase());
			if (EmptyKit.isNotBlank(this.getAdditionalString())) {
				sb.append("?").append(this.getAdditionalString().trim());
			}
			return sb.toString();
		}
	}

	public String getDatabase() {
		if (isUri && EmptyKit.isNotEmpty(uri)) {
			ConnectionString connectionString = new ConnectionString(uri);
			return connectionString.getDatabase();
		} else {
			return database;
		}
	}

	public List<String> getHosts() {
		ConnectionString connectionString = new ConnectionString(getUri());
		return connectionString.getHosts();
	}

	@Override
	public String getConnectionString() {
		return getUri();
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public void setIsUri(boolean uri) {
		isUri = uri;
	}

	public boolean getIsUri() {
		return isUri;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getAdditionalString() {
		return additionalString;
	}

	public void setAdditionalString(String additionalString) {
		this.additionalString = additionalString;
	}

	public boolean isSsl() {
		return ssl;
	}

	public void setSsl(boolean ssl) {
		this.ssl = ssl;
	}

	public String getSslKey() {
		return sslKey;
	}

	public void setSslKey(String sslKey) {
		this.sslKey = sslKey;
	}

	public String getSslPass() {
		return sslPass;
	}

	public void setSslPass(String sslPass) {
		this.sslPass = sslPass;
	}

	public boolean isSslValidate() {
		return sslValidate;
	}

	public void setSslValidate(boolean sslValidate) {
		this.sslValidate = sslValidate;
	}

	public boolean getCheckServerIdentity() {
		return checkServerIdentity;
	}

	public void setCheckServerIdentity(boolean checkServerIdentity) {
		this.checkServerIdentity = checkServerIdentity;
	}

	public String getSslCA() {
		return sslCA;
	}

	public void setSslCA(String sslCA) {
		this.sslCA = sslCA;
	}

	public String getInsertDmlPolicy() {
		return insertDmlPolicy;
	}

	public void setInsertDmlPolicy(String insertDmlPolicy) {
		this.insertDmlPolicy = insertDmlPolicy;
	}

	public String getUpdateDmlPolicy() {
		return updateDmlPolicy;
	}

	public void setUpdateDmlPolicy(String updateDmlPolicy) {
		this.updateDmlPolicy = updateDmlPolicy;
	}

	public boolean isEnableSaveDeleteData() {
		return enableSaveDeleteData;
	}

	public void setEnableSaveDeleteData(boolean enableSaveDeleteData) {
		this.enableSaveDeleteData = enableSaveDeleteData;
	}

	public boolean isEnableFillingModifiedData() {
		return enableFillingModifiedData;
	}

	public void setEnableFillingModifiedData(boolean enableFillingModifiedData) {
		this.enableFillingModifiedData = enableFillingModifiedData;
	}

	public boolean isNoCursorTimeout() {
		return noCursorTimeout;
	}

	public void setNoCursorTimeout(boolean noCursorTimeout) {
		this.noCursorTimeout = noCursorTimeout;
	}
}

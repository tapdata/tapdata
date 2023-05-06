package com.tapdata.constant;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.tapdata.entity.BulkOpResult;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DataQualityTag;
import com.tapdata.entity.DataRules;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MongodbShardKeyInfo;
import com.tapdata.entity.Stats;
import com.tapdata.entity.TableIndex;
import com.tapdata.entity.TableIndexColumn;
import com.tapdata.entity.Worker;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.mongo.BigDecimalCodec;
import com.tapdata.mongo.BigIntegerCodec;
import com.tapdata.mongo.ByteArrayCodec;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.DateCodec;
import com.tapdata.mongo.FloatCodec;
import com.tapdata.mongo.ObjectIdCodec;
import com.tapdata.mongo.StringCodec;
import com.tapdata.mongo.UndifinedCodec;
import io.tapdata.exception.BaseDatabaseUtilException;
import io.tapdata.exception.MongodbException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tapdata.entity.DataQualityTag.SUB_COLUMN_NAME;

/**
 * Created by tapdata on 08/02/2018.
 */
public class MongodbUtil extends BaseDatabaseUtil {

	private static Logger logger = LogManager.getLogger(MongodbUtil.class);

	private final static String BUILDINFO = "buildinfo";
	private final static String VERSION = "version";
	private final static int MONGODB_VERSION_4 = 4;

	private final static String GET_MONGODB_DATE = "new Date()";

	private static final int SAMPLE_SIZE_BATCH_SIZE = 1000;

	public static final Set<String> systemTables = new HashSet<>();

	static {
		systemTables.add("system.views");
		systemTables.add("system.indexes");
		systemTables.add("system.profile");
		systemTables.add("system.namespaces");
		systemTables.add("system.js");
	}

	public static MongoClient createMongoClient(Connections connection) throws UnsupportedEncodingException {
		return createMongoClient(connection, MongoClientOptions.builder().build());
	}

	public static MongoClient createMongoClient(Connections connection, MongoClientOptions mongoClientOptions)
			throws UnsupportedEncodingException {
		MongoClient mongoClient;
		try {

			String host = connection.getDatabase_host();
			Integer port = connection.getDatabase_port();
			String username = connection.getDatabase_username();
			String password = connection.getDatabase_password();
			String databaseUri = connection.getDatabase_uri();
			String authDb = connection.getAuth_db();
			String databaseName = connection.getDatabase_name();
			String additionalString = connection.getAdditionalString();

			MongoClientOptions.Builder builder;
			if (null != mongoClientOptions) {
				builder = MongoClientOptions.builder(mongoClientOptions);
			} else {
				builder = MongoClientOptions.builder();
			}

			// ssl config
			if (connection.getSsl()) {

				if (StringUtils.isNotEmpty(databaseUri) &&
						(databaseUri.indexOf("tlsAllowInvalidCertificates=true") > 0 ||
								databaseUri.indexOf("sslAllowInvalidCertificates=true") > 0)) {
					SSLContext sslContext = SSLContext.getInstance("SSL");
					sslContext.init(null, new TrustManager[]{new X509TrustManager() {
						@Override
						public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
						}

						@Override
						public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
						}

						@Override
						public X509Certificate[] getAcceptedIssuers() {
							return null;
						}
					}}, new SecureRandom());
					builder.sslEnabled(true).sslContext(sslContext).sslInvalidHostNameAllowed(true);

				} else {
					sslMongoClientOption(connection.getSslValidate(), connection.getSslCA(), connection.getSslCert(),
							connection.getSslKey(), connection.getSslPass(), connection.getCheckServerIdentity(), builder);
				}
			}

			builder.cursorFinalizerEnabled(false);

			if (StringUtils.isNotEmpty(databaseUri)) {
				MongoClientURI uri = new MongoClientURI(databaseUri, builder);
				mongoClient = new MongoClientProxy(uri);
			} else {
				StringBuilder sb = new StringBuilder("mongodb://");
				if (StringUtils.isNoneBlank(username, password)) {
					sb.append(URLEncoder.encode(username, "UTF-8")).append(":").append(URLEncoder.encode(password, "UTF-8")).append("@");
				}
				if (host.contains(":")) {
					sb.append(host);
				} else {
					sb.append(host).append(":").append(port);
				}
				sb.append("/");
				if (StringUtils.isNotBlank(databaseName)) {
					sb.append(databaseName);
				}

				// additional string
				if (StringUtils.isNoneBlank(authDb)) {
					sb.append("?authSource=").append(authDb);
					if (StringUtils.isNotBlank(additionalString)) {
						if (StringUtils.startsWith(additionalString, "&")) {
							sb.append(additionalString);
						} else {
							sb.append("&").append(additionalString);
						}
					}
				} else {
					if (StringUtils.isNotBlank(additionalString)) {
						sb.append("?");
						if (StringUtils.startsWith(additionalString, "&")) {
							sb.append(additionalString.substring(1));
						} else {
							sb.append(additionalString);
						}
					}
				}
				MongoClientURI uri = new MongoClientURI(sb.toString(), builder);
				mongoClient = new MongoClientProxy(uri);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return mongoClient;
	}

	public static String getUniqueName(Connections connections) throws Exception {
		String name;

		if (connections == null) {
			throw new IllegalArgumentException("Missing connections");
		}

		try {

			final String databaseUri = connections.getDatabase_uri();
			MongoClientURI mongoClientURI = new MongoClientURI(databaseUri);

			List<String> hostPorts = mongoClientURI.getHosts();
			String hostString = hostPorts.stream().map(hostPort -> {
				String result;
				String[] split = hostPort.split(":");
				if (split.length > 1) {
					String host = split[0];
					host = NetworkUtil.hostname2IpAddress(host);
					result = host + ":" + split[1];
				} else {
					result = NetworkUtil.hostname2IpAddress(hostPort) + ":27017";
				}
				return result;
			}).collect(Collectors.joining(","));
			name = hostString + "/" + mongoClientURI.getDatabase();
		} catch (Exception throwable) {
			throw new Exception(
					String.format(
							"Get connection name %s unique name error: %s",
							connections.getName(),
							throwable.getMessage()
					),
					throwable
			);
		}

		return name;
	}

	public static String getMongoDBURIOptions(String databaseUri) {
		String options = null;
		try {

			if (StringUtils.isNotBlank(databaseUri)) {
				String[] split = databaseUri.split("\\?", 2);
				if (split.length == 2) {
					options = split[1];
				}
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return options;
	}

	public static MongoClient createMongoClient(Connections connection, CodecRegistry codecRegistry) throws UnsupportedEncodingException {
		MongoClientOptions.Builder builder = MongoClientOptions.builder();
		if (null != codecRegistry) {
			builder.codecRegistry(codecRegistry);
		}

		return createMongoClient(connection, builder.build());
	}

	public static void sslMongoClientOption(boolean sslValidate, String sslCA, String sslCert, String sslKeyStr, String sslPass,
											boolean checkServerIdentity, MongoClientOptions.Builder builder) throws Exception {
		List<String> trustCertificates = null;
		if (sslValidate) {
			trustCertificates = SSLUtil.retriveCertificates(sslCA);
		}
		List<String> certificates = SSLUtil.retriveCertificates(sslCert);
		String sslKey = SSLUtil.retrivePrivateKey(sslKeyStr);
		if (StringUtils.isNotBlank(sslKey) && CollectionUtils.isNotEmpty(certificates)) {
			SSLContext sslContext = SSLUtil.createSSLContext(sslKey, certificates, trustCertificates, sslPass);
			builder.sslContext(sslContext);
			builder.sslEnabled(true);
			builder.sslInvalidHostNameAllowed(!checkServerIdentity);
		}
	}

	public static CodecRegistry getForJavaCoedcRegistry() {
		Map<BsonType, Class<?>> replacments = new HashMap<>();
		replacments.put(BsonType.DECIMAL128, BigDecimal.class);
		replacments.put(BsonType.BINARY, byte[].class);
		replacments.put(BsonType.BOOLEAN, Boolean.class);
		replacments.put(BsonType.DATE_TIME, Date.class);
		replacments.put(BsonType.JAVASCRIPT, String.class);
		replacments.put(BsonType.JAVASCRIPT_WITH_SCOPE, String.class);
		replacments.put(BsonType.STRING, String.class);
		replacments.put(BsonType.SYMBOL, String.class);
		replacments.put(BsonType.TIMESTAMP, Date.class);

		CodecRegistry codecRegistry = MongodbUtil.customCodecRegistry(
				Arrays.asList(
						new BigIntegerCodec(), new BigDecimalCodec(), new FloatCodec(), new UndifinedCodec(), new ByteArrayCodec(),
						new DateCodec(), new StringCodec()
				),
				replacments
		);

		return codecRegistry;
	}

	public static CodecRegistry getForNodejsCoedcRegistry() {
		Map<BsonType, Class<?>> replacments = new HashMap<>();
		replacments.put(BsonType.DECIMAL128, BigDecimal.class);
		replacments.put(BsonType.BINARY, byte[].class);
		replacments.put(BsonType.BOOLEAN, Boolean.class);
		replacments.put(BsonType.DATE_TIME, Date.class);
		replacments.put(BsonType.JAVASCRIPT, String.class);
		replacments.put(BsonType.JAVASCRIPT_WITH_SCOPE, String.class);
		replacments.put(BsonType.STRING, String.class);
		replacments.put(BsonType.SYMBOL, String.class);
		replacments.put(BsonType.TIMESTAMP, Date.class);
		replacments.put(BsonType.OBJECT_ID, String.class);

		CodecRegistry codecRegistry = MongodbUtil.customCodecRegistry(
				Arrays.asList(
						new BigIntegerCodec(), new BigDecimalCodec(), new FloatCodec(), new UndifinedCodec(), new ByteArrayCodec(),
						new DateCodec(), new StringCodec(), new ObjectIdCodec()
				),
				replacments
		);

		return codecRegistry;
	}

	public static long mongodbServerTimestamp(MongoClient mongoClient) {

		return mongodbServerTimestamp(mongoClient.getDatabase("test"));
	}

	public static long mongodbServerTimestamp(MongoDatabase mongoDatabase) {
		Document result = mongoDatabase.runCommand(new Document("isMaster", 1));
		Date serverDate = result.getDate("localTime");
		if (result.containsKey("$clusterTime")) {
			Object clusterTime = result.get("$clusterTime", Document.class).get("clusterTime");
			if (clusterTime instanceof Date) {
				serverDate = (Date) clusterTime;
			} else {
				BsonTimestamp bsonTimestamp = (BsonTimestamp) clusterTime;
				serverDate = new Date(bsonTimestamp.getTime() * 1000L);
			}
		}
		return serverDate != null ? serverDate.getTime() : 0;
	}

	public static CodecRegistry customCodecRegistry(List<Codec<?>> codecs, Map<BsonType, Class<?>> replacementsForDefaults) {

		BsonTypeClassMap bsonTypeCodecMap = new BsonTypeClassMap(replacementsForDefaults);
		DocumentCodecProvider documentCodecProvider = new DocumentCodecProvider(bsonTypeCodecMap);

		CodecRegistry defaultCodecRegistry = MongoClient.getDefaultCodecRegistry();

		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
				CodecRegistries.fromCodecs(codecs),
				CodecRegistries.fromProviders(documentCodecProvider),
				defaultCodecRegistry
		);

		return codecRegistry;
	}

	public static class DocumentCodecProvider implements CodecProvider {
		private final BsonTypeClassMap bsonTypeClassMap;

		public DocumentCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
			this.bsonTypeClassMap = bsonTypeClassMap;
		}

		@Override
		public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
			if (clazz == Document.class) {
				// construct DocumentCodec with a CodecRegistry and a BsonTypeClassMap
				return (Codec<T>) new DocumentCodec(registry, bsonTypeClassMap);
			}

			return null;
		}
	}

	public static String getDatabase(Connections connections) {

		String databaseName = connections.getDatabase_name();

		String databaseUri = connections.getDatabase_uri();
		if (StringUtils.isNotBlank(databaseUri)) {
			databaseName = getDatabase(databaseUri);
		}
		return databaseName;
	}

	public static String getDatabase(String mongodbURI) {

		String databaseName = null;

		if (StringUtils.isNotBlank(mongodbURI)) {
			MongoClientURI uri = new MongoClientURI(mongodbURI);
			databaseName = uri.getDatabase();
		}
		return databaseName;
	}

	public static Map<String, String> nodesURI(Connections connection) throws UnsupportedEncodingException {
		Map<String, String> nodeConnURIs = new HashMap<>();

		String username = connection.getDatabase_username();
		String password = connection.getDatabase_password();
		String host = connection.getDatabase_host();
		Integer port = connection.getDatabase_port();
		String databaseName = connection.getDatabase_name();

		List<String> hosts = new ArrayList<>();
		hosts.add(host + ":" + port);
		String databaseUri = connection.getDatabase_uri();
		if (StringUtils.isNotBlank(databaseUri)) {
			hosts.clear();
			MongoClientURI uri = new MongoClientURI(databaseUri);
			username = uri.getUsername();
			char[] passChars = uri.getPassword();
			if (passChars != null && passChars.length > 0) {
				password = new String(passChars);
			}
			hosts = uri.getHosts();

			databaseName = uri.getDatabase();
		}

		MongoClient mongoClient = null;
		MongoCursor<Document> cursor = null;
		try {
			String mongoDBURIOptions = getMongoDBURIOptions(connection.getDatabase_uri());

			mongoClient = createMongoClient(connection);
			MongoCollection<Document> collection = mongoClient.getDatabase("config").getCollection("shards");
			cursor = collection.find().iterator();
			while (cursor.hasNext()) {
				Document doc = cursor.next();
				String hostStr = doc.getString("host");
				String replicaSetName = replicaSetUsedIn(hostStr);
				String addresses = hostStr.split("/")[1];
				StringBuilder sb = new StringBuilder();
				if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
					sb.append("mongodb://").append(URLEncoder.encode(username, "UTF-8")).append(":").append(URLEncoder.encode(password, "UTF-8")).append("@").append(addresses).append("/").append(databaseName);
				} else {
					sb.append("mongodb://").append(addresses).append("/").append(databaseName);
				}
				if (StringUtils.isNotBlank(mongoDBURIOptions)) {
					sb.append("?").append(mongoDBURIOptions);
				}
				nodeConnURIs.put(replicaSetName, sb.toString());
			}

			if (nodeConnURIs.size() == 0) {
				// The addresses may be a replica set ...
				try {
					Document document = mongoClient.getDatabase("admin").runCommand(new Document("replSetGetStatus", 1));
					List members = document.get("members", List.class);
					if (CollectionUtils.isNotEmpty(members)) {

						StringBuilder sb = new StringBuilder();
						// This is a replica set ...
						for (Object member : members) {
							Document doc = (Document) member;
							sb.append(doc.getString("name")).append(",");
						}
						String addressStr = sb.deleteCharAt(sb.length() - 1).toString();
						String replicaSetName = document.getString("set");

						StringBuilder uriSB = new StringBuilder();
						if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
							uriSB.append("mongodb://").append(URLEncoder.encode(username, "UTF-8")).append(":").append(URLEncoder.encode(password, "UTF-8")).append("@").append(addressStr).append("/").append(databaseName);
						} else {
							uriSB.append("mongodb://").append(addressStr).append("/").append(databaseName);
						}
						if (StringUtils.isNotBlank(mongoDBURIOptions)) {
							uriSB.append("?").append(mongoDBURIOptions);
						}
						nodeConnURIs.put(replicaSetName, uriSB.toString());
					}
				} catch (Exception e) {
					String replicaSetName = "single";
					if (replicaSetName != null) {

						for (String address : hosts) {
							StringBuilder sb = new StringBuilder();
							if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
								sb.append("mongodb://").append(URLEncoder.encode(username, "UTF-8")).append(":").append(URLEncoder.encode(password, "UTF-8")).append("@").append(address).append("/").append(databaseName);
							} else {
								sb.append("mongodb://").append(address).append("/").append(databaseName);
							}
							if (StringUtils.isNotBlank(mongoDBURIOptions)) {
								sb.append("?").append(mongoDBURIOptions);
							}
							nodeConnURIs.put(replicaSetName, sb.toString());
						}
					}
				}
			}
		} finally {

			releaseConnection(mongoClient, cursor);
		}

		return nodeConnURIs;
	}

	public static Map<String, String> nodesURI(Connections connection, String serverSelectionTimeoutMS) throws UnsupportedEncodingException {
		Map<String, String> nodesURI = new HashMap<>();
		if (StringUtils.isNotBlank(connection.getDatabase_uri())) {
			String databaseUri = connection.getDatabase_uri();
			MongoClientURI mongoClientURI = new MongoClientURI(databaseUri);
			List<String> hosts = mongoClientURI.getHosts();
			String hostPort = "";
			for (String host : hosts) {
				hostPort += host + ",";
			}
			if (StringUtils.endsWithIgnoreCase(hostPort, ",")) {
				hostPort = StringUtils.removeEnd(hostPort, ",");
			}
			connection.setDatabase_uri(appendMongoUri(
					hostPort,
					mongoClientURI.getUsername(),
					mongoClientURI.getPassword(),
					serverSelectionTimeoutMS,
					"admin"
			));

			nodesURI = nodesURI(connection);
		}
		return nodesURI;
	}

	public static BsonTimestamp getOplogTimestamp(Connections connection, int sort) throws UnsupportedEncodingException {
		BsonTimestamp latestTimestamp = null;
		Map<String, String> nodesURI = nodesURI(connection);
		for (Map.Entry<String, String> entry : nodesURI.entrySet()) {
			MongoClient mongoClient = null;
			try {
				mongoClient = createMongoClient(entry.getValue());

				MongoCollection<Document> oplog = mongoClient.getDatabase("local").getCollection("oplog.rs");
				Document firstEvent = oplog.find().sort(new Document("$natural", sort)).limit(1).first();
				if (firstEvent != null) {
					BsonTimestamp firstEventTs = firstEvent.get("ts", BsonTimestamp.class);

					if (latestTimestamp == null) {
						latestTimestamp = firstEventTs;
					} else if (latestTimestamp.compareTo(firstEventTs) == sort) {
						latestTimestamp = firstEventTs;
					}
				}
			} finally {
				releaseConnection(mongoClient, null);
			}
		}

		return latestTimestamp;
	}

	public static BsonTimestamp getOplogTimestamp(Connections connection, String namespace, int sort) throws UnsupportedEncodingException {
		BsonTimestamp latestTimestamp = null;
		Map<String, String> nodesURI = nodesURI(connection);
		for (Map.Entry<String, String> entry : nodesURI.entrySet()) {
			MongoClient mongoClient = null;
			try {
				mongoClient = createMongoClient(entry.getValue());

				MongoCollection<Document> oplog = mongoClient.getDatabase("local").getCollection("oplog.rs");
				Document filter = new Document();
				if (StringUtils.isNotBlank(namespace)) {
					filter.append("ns", namespace);
				}

				Document firstEvent = oplog.find(filter).sort(new Document("$natural", sort)).limit(1).first();
				if (firstEvent != null) {
					BsonTimestamp firstEventTs = firstEvent.get("ts", BsonTimestamp.class);

					if (latestTimestamp == null) {
						latestTimestamp = firstEventTs;
					} else if (latestTimestamp.compareTo(firstEventTs) == sort) {
						latestTimestamp = firstEventTs;
					}
				}
			} finally {
				releaseConnection(mongoClient, null);
			}
		}

		return latestTimestamp;
	}

	public static ChangeStreamDocument<Document> getChangeStreamCurrentEvent(MongoClient mongoClient, String database) {

		try (final MongoCursor<ChangeStreamDocument<Document>> mongoCursor = mongoClient.getDatabase(database).watch().iterator()) {
			final ChangeStreamDocument<Document> changeStreamDocument = mongoCursor.tryNext();
			if (changeStreamDocument != null) {
				return changeStreamDocument;
			}
		} catch (MongoCommandException e) {
			// "code" : 136, "codeName" : "CappedPositionLost"
			// compatible this mongodb server issue: https://jira.mongodb.org/browse/SERVER-48523
			if (136 == e.getErrorCode()) {
				return null;
			} else {
				throw e;
			}
		}
		return null;
	}

	public static ChangeStreamDocument<Document> getChangeStreamCurrentEvent(MongoClient mongoClient) {

		final long serverTimestamp = mongodbServerTimestamp(mongoClient);
		if (serverTimestamp > 0) {
			BsonTimestamp bsonTimestamp = new BsonTimestamp((int) (serverTimestamp / 1000), 0);
			try (final MongoCursor<ChangeStreamDocument<Document>> mongoCursor = mongoClient.watch().startAtOperationTime(bsonTimestamp).iterator()) {
				final ChangeStreamDocument<Document> changeStreamDocument = mongoCursor.tryNext();
				if (changeStreamDocument != null) {
					return changeStreamDocument;
				}
			}
		}
		return null;
	}

	public static Map<String, List<String>> getShardKeysByMappings(List<Mapping> mappings, Connections connections) {
		Map<String, List<String>> tablesShardKeys = null;
		if (CollectionUtils.isNotEmpty(mappings)) {
			MongoClient mongoClient = null;
			MongoCursor<Document> mongoCursor = null;
			try {
				tablesShardKeys = new HashMap<>();
				String database = getDatabase(connections);
				mongoClient = createMongoClient(connections);
				List<Document> tablesOrQuery = new ArrayList<>();
				for (Mapping mapping : mappings) {
					tablesOrQuery.add(new Document("_id", database + "." + mapping.getTo_table()));
				}

				Document query = new Document("$or", tablesOrQuery);

				MongoCollection<Document> collection = mongoClient.getDatabase("config").getCollection("collections");
				mongoCursor = collection.find(query).iterator();

				while (mongoCursor.hasNext()) {
					Document shardInfo = mongoCursor.next();

					List<String> shardKeys = new ArrayList<>();
					Document key = shardInfo.get("key", Document.class);
					if (MapUtils.isEmpty(key)) {
						continue;
					}
					for (String shardKey : key.keySet()) {
						shardKeys.add(shardKey);
					}
					Object id = shardInfo.get("_id");
					if (!(id instanceof String) || StringUtils.isBlank((String) id)) {
						continue;
					}
					String ns = (String) id;
					String collectionName = ns.split("\\.")[1];
					tablesShardKeys.put(collectionName, shardKeys);
				}
			} catch (Exception e) {
				logger.warn("Checking collection shard key info failed {}, stacks: {}", e.getMessage(), Log4jUtil.getStackString(e));
			} finally {
				releaseConnection(mongoClient, mongoCursor);
			}
		}
		return tablesShardKeys;
	}

	public static Map<String, MongodbShardKeyInfo> listShardKeys(MongoCollection<Document> collectionsDocument, String dbName, Set<String> includeTableNames) {
		Map<String, MongodbShardKeyInfo> shardKeys = new HashMap<>();
		try {
			// 不为 null 查指定表，为 null 查所有表
			FindIterable<Document> findIterable;
			if (null != includeTableNames) {
				List<Document> tablesOrQuery = new ArrayList<>();
				for (String tn : includeTableNames) {
					tablesOrQuery.add(new Document("_id", dbName + "." + tn));
				}
				Document query = new Document("$or", tablesOrQuery);
				findIterable = collectionsDocument.find(query);
			} else {
				Document query = new Document("_id", new Document("$regex", "^" + dbName + "\\."));
				findIterable = collectionsDocument.find(query);
			}

			try (MongoCursor<Document> mongoCursor = findIterable.iterator()) {
				MongodbShardKeyInfo shardKeyInfo;
				while (mongoCursor.hasNext()) {
					Document shardInfo = mongoCursor.next();
					shardKeyInfo = MongodbShardKeyInfo.parse(shardInfo);
					if (null != shardKeyInfo) {
						shardKeys.put(shardKeyInfo.collectionName(), shardKeyInfo);
					}
				}
			}
		} catch (Exception e) {
			logger.warn("Load collection shard key info failed {}, stacks: {}", e.getMessage(), Log4jUtil.getStackString(e));
		}
		return shardKeys;
	}

	public static MongoClient createMongoClient(String uriStr) {
		return createMongoClient(new MongoClientURI(uriStr));
	}

	public static MongoClient createMongoClient(MongoClientURI mongoClientURI) {
		return new MongoClientProxy(mongoClientURI);
	}

	public static long getDBCount(Connections connection) throws UnsupportedEncodingException {

		long dbCount = 0l;
		MongoClient sourceMongoClient = null;
		try {
			sourceMongoClient = MongodbUtil.createMongoClient(connection);
			dbCount = getDBCount(connection, sourceMongoClient);

		} finally {
			releaseConnection(sourceMongoClient, null);
		}

		return dbCount;
	}

	public static long getDBCount(Connections connection, MongoClient mongoClient) {
		long dbCount = 0l;
		String sourceDB = null;
		String databaseUri = connection.getDatabase_uri();
		if (StringUtils.isNotBlank(databaseUri)) {
			MongoClientURI uri = new MongoClientURI(databaseUri);
			sourceDB = uri.getDatabase();
		} else {
			sourceDB = connection.getDatabase_name();
		}

		if (StringUtils.isBlank(sourceDB)) {
			return dbCount;
		}
		MongoDatabase database = mongoClient.getDatabase(sourceDB);
		MongoCursor<String> iterator = database.listCollectionNames().iterator();
		while (iterator.hasNext()) {
			String collection = iterator.next();
			if (systemTables.contains(collection)) {
				continue;
			}
			Document document = database.runCommand(new Document("collStats", collection));
			Object count = document.get("count");
			long collCount = 0L;
			if (count instanceof Integer) {
				collCount += new BigDecimal((Integer) count).longValue();
			} else if (count instanceof Double) {
				collCount += new BigDecimal((Double) count).longValue();
			}
			dbCount += collCount;
		}

		return dbCount;
	}

	public static String replicaSetUsedIn(String addresses) {
		if (addresses.startsWith("[")) {
			// Just an IPv6 address, so no replica set name ...
			return null;
		}
		// Either a replica set name + an address, or just an IPv4 address ...
		int index = addresses.indexOf('/');
		if (index < 0) return null;
		return addresses.substring(0, index);
	}

	/**
	 * Returns a new String composed of the supplied values joined together with a copy of the specified {@code delimiter}.
	 * All {@code null} values are simply ignored.
	 *
	 * @param delimiter the delimiter that separates each element
	 * @param values    the values to join together.
	 * @return a new {@code String} that is composed of the {@code elements} separated by the {@code delimiter}
	 * @throws NullPointerException If {@code delimiter} or {@code elements} is {@code null}
	 * @see java.lang.String#join
	 */
	public static <T> String join(CharSequence delimiter, Iterable<T> values) {
		return join(delimiter, values, v -> {
			return v != null ? v.toString() : null;
		});
	}

	/**
	 * Returns a new String composed of the supplied values joined together with a copy of the specified {@code delimiter}.
	 *
	 * @param delimiter  the delimiter that separates each element
	 * @param values     the values to join together.
	 * @param conversion the function that converts the supplied values into strings, or returns {@code null} if the value
	 *                   is to be excluded
	 * @return a new {@code String} that is composed of the {@code elements} separated by the {@code delimiter}
	 * @throws NullPointerException If {@code delimiter} or {@code elements} is {@code null}
	 * @see java.lang.String#join
	 */
	public static <T> String join(CharSequence delimiter, Iterable<T> values, Function<T, String> conversion) {
		Objects.requireNonNull(delimiter);
		Objects.requireNonNull(values);
		Iterator<T> iter = values.iterator();
		if (!iter.hasNext()) return "";
		StringBuilder sb = new StringBuilder();
		sb.append(iter.next());
		while (iter.hasNext()) {
			String convertedValue = conversion.apply(iter.next());
			if (convertedValue != null) {
				sb.append(delimiter);
				sb.append(convertedValue);
			}
		}
		return sb.toString();
	}

	/**
	 * @param job
	 * @param jobTargetConn
	 * @param pk_filter     true: drop all collections
	 *                      false: only drop collections which without join conditions(primary/unique keys)
	 * @return
	 * @throws IOException
	 */
	public static String dropMongoTargetCollections(Job job, Connections jobTargetConn, boolean pk_filter) throws IOException {
		String collections;
		MongoClient mongoClient = null;
		try {
			// drop target collection
			mongoClient = MongodbUtil.createMongoClient(jobTargetConn);
			String database = jobTargetConn.getDatabase_name();
			String databaseUri = jobTargetConn.getDatabase_uri();
			if (StringUtils.isNoneEmpty(databaseUri)) {
				MongoClientURI uri = new MongoClientURI(databaseUri);
				database = uri.getDatabase();
			}
			collections = MongodbUtil.dropTargetByMappings(job, mongoClient, database, pk_filter);
		} finally {
			releaseConnection(mongoClient, null);
		}

		return collections;
	}

	private static String dropTargetByMappings(Job job, MongoClient mongoClient, String databaseName, boolean pk_filter) {
		List<Mapping> mappings = job.getMappings();
		StringBuffer collections = new StringBuffer();
		String returnStr;
		MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
		if (CollectionUtils.isNotEmpty(mappings)) {
			Set<String> dropCollections = new HashSet<>();
			if (job.isOnlyInitialAddMapping()) {
				mappings = job.getAddInitialMapping();
			}
			int loopCounter = 0;
			mappings = mappings.stream().filter(Mapping::getDropTarget).collect(Collectors.toList());
			for (Mapping mapping : mappings) {
				if (!job.isRunning()) {
					break;
				}
				List<Map<String, String>> joinCondition = mapping.getJoin_condition();

				if (pk_filter && CollectionUtils.isNotEmpty(joinCondition)) {
					continue;
				}

				String collectionName = mapping.getTo_table();

				dropCollections.add(collectionName);
			}

			// 加载原片健
			MongoDatabase configDatabase = mongoClient.getDatabase("config");
			MongoCollection<Document> collectionsDocument = configDatabase.getCollection("collections");
			Map<String, MongodbShardKeyInfo> shardKeyInfos = MongodbUtil.listShardKeys(collectionsDocument, databaseName, dropCollections);

			MongoDatabase adminDatabase = mongoClient.getDatabase("admin");
			for (String dropCollection : dropCollections) {
				if (!job.isRunning()) {
					break;
				}
				if (StringUtils.isNotBlank(dropCollection)) {
					String droppedCollection = dropCollectionKeepIndexAndShardKey(mongoDatabase, dropCollection, adminDatabase, shardKeyInfos.get(dropCollection));
					if (StringUtils.isNotBlank(droppedCollection)) {
						collections.append(droppedCollection).append(",");
					}
				}
				if ((++loopCounter) % ConnectorConstant.LOOP_BATCH_SIZE == 0) {
					logger.info("Drop mongoDB collection progress: " + loopCounter + "/" + mappings.size());
				}
			}

			returnStr = StringUtils.removeEnd(collections.toString(), ",");
		} else {
			collections = collections.append("Found no collections need to drop");
			returnStr = collections.toString();
		}
		return returnStr;
	}

	public static String dropCollectionKeepIndexAndShardKey(MongoDatabase mongoDatabase, String dropCollection, MongoDatabase adminDatabase, MongodbShardKeyInfo shardKeyInfo) {
		String droppedCollection = null;
		try {
			MongoCollection mongoCollection = mongoDatabase.getCollection(dropCollection);
			ListIndexesIterable<Document> listIndexesIterable = mongoCollection.listIndexes(Document.class);
			List<IndexModel> indexModels = new ArrayList<>();
			for (Document document : listIndexesIterable) {
				BsonDocument bsonDocument = new BsonDocument();
				Map<String, Object> keyMap = (Map<String, Object>) document.get("key");
				for (Map.Entry<String, Object> map : keyMap.entrySet()) {
					Object value = map.getValue();
					BsonValue bsonValue = typeConversion(value);
					if (bsonValue != null) {
						bsonDocument.append(map.getKey(), bsonValue);
					}
				}
				if (bsonDocument.size() > 0) {
					IndexOptions indexOptions = buildIndexOptions(document);
					indexModels.add(new IndexModel(bsonDocument, indexOptions));
				}
			}

			mongoCollection.drop();
			droppedCollection = dropCollection;
			if (!indexModels.isEmpty()) {
				// create collection
				mongoDatabase.createCollection(dropCollection);
				// create indexes
				mongoCollection.createIndexes(indexModels);
				// create shard key
				if (null != shardKeyInfo) {
					adminDatabase.runCommand(shardKeyInfo.toCreateDocument());
				}
			}
		} catch (Exception e) {
			logger.warn(
					"Auto create target collection {}'s indexes failed {}, when dropped collection.",
					dropCollection,
					e.getMessage(),
					e
			);
		}

		return droppedCollection;
	}

	public static IndexOptions buildIndexOptions(Document document) {
		IndexOptions indexOptions = new IndexOptions();
		if (document.get("background") != null) {

			boolean background = object2Boolean(document.get("background"));

			indexOptions.background(background);
		}
		if (document.get("unique") != null) {
			boolean background = object2Boolean(document.get("unique"));
			indexOptions.unique(background);
		}
		if (document.get("name") != null) {
			indexOptions.name(document.getString("name"));
		}
		if (document.get("sparse") != null) {
			boolean sparse = object2Boolean(document.get("sparse"));
			indexOptions.sparse(sparse);
		}
		if (document.get("expireAfterSeconds") != null) {
			indexOptions.expireAfter(Double.valueOf(document.get("expireAfterSeconds").toString()).longValue(), TimeUnit.SECONDS);
		}
		if (document.get("v") != null) {
			indexOptions.version(document.getInteger("v"));
		}
		if (document.get("weights") != null) {
			if (document.get("weights") instanceof Bson) {
				indexOptions.weights((Bson) document.get("weights"));
			} else {
				logger.warn("mongo index weights is not bson...");
				indexOptions.weights(new Document("weights", document.get("weights")));
			}
		}
		if (document.get("default_language") != null) {
			indexOptions.defaultLanguage(document.getString("default_language"));
		}
		if (document.get("language_override") != null) {
			indexOptions.languageOverride(document.getString("language_override"));
		}
		if (document.get("textIndexVersion") != null) {
			indexOptions.textVersion(document.getInteger("textIndexVersion"));
		}
		if (document.get("2dsphereIndexVersion") != null) {
			indexOptions.sphereVersion(document.getInteger("2dsphereIndexVersion"));
		}
		if (document.get("bits") != null) {
			indexOptions.bits(document.getInteger("bits"));
		}
		if (document.get("min") != null) {
			indexOptions.min(document.getDouble("min"));
		}
		if (document.get("max") != null) {
			indexOptions.max(document.getDouble("max"));
		}
		if (document.get("bucketSize") != null) {
			indexOptions.bucketSize(document.getDouble("bucketSize"));
		}
		if (document.get("storageEngine") != null) {
			if (document.get("storageEngine") instanceof Bson) {
				indexOptions.storageEngine((Bson) document.get("storageEngine"));
			} else {
				logger.warn("mongo index storageEngine is not bson...");
				indexOptions.storageEngine(new Document("storageEngine", document.get("storageEngine")));
			}
		}
		if (document.get("partialFilterExpression") != null) {
			if (document.get("partialFilterExpression") instanceof Bson) {
				indexOptions.partialFilterExpression((Bson) document.get("partialFilterExpression"));
			} else {
				logger.warn("mongo index partialFilterExpression is not bson...");
				indexOptions.partialFilterExpression(new Document("partialFilterExpression", document.get("partialFilterExpression")));
			}
		}
		if (document.get("collation") != null) {
			if (document.get("collation") instanceof Collation) {
				indexOptions.collation((Collation) document.get("collation"));
			} else if (document.get("collation") instanceof Bson) {
				logger.warn("mongo index collation is not Collation and is bson...");
				indexOptions.collation(getCollation((Document) document.get("collation")));
			}
		}
		return indexOptions;
	}

	private static boolean object2Boolean(Object object) {
		boolean result = false;

		if (object == null) {
			return result;
		}

		if (object instanceof Double) {
			result = ((Double) object) > 0;
		} else {
			result = (Boolean) object;
		}
		return result;
	}

	public static BsonValue typeConversion(Object value) {
		BsonValue bsonValue = null;
		if (value instanceof Integer) {
			bsonValue = new BsonInt32((Integer) value);
		} else if (value instanceof Long) {
			bsonValue = new BsonInt64((Long) value);
		} else if (value instanceof Double) {
			bsonValue = new BsonDouble((Double) value);
		} else if (value instanceof String) {
			bsonValue = new BsonString((String) value);
		}

		return bsonValue;
	}

	public static Collation getCollation(Document document) {
		Collation.Builder builder = Collation.builder();
		if (document.get("locale") != null) {
			builder.locale(document.getString("locale"));
		}
		if (document.get("caseLevel") != null) {
			builder.caseLevel(document.getBoolean("caseLevel"));
		}
		if (document.get("caseFirst") != null) {
			builder.collationCaseFirst(CollationCaseFirst.fromString(document.getString("caseFirst")));
		}
		if (document.get("strength") != null) {
			builder.collationStrength(CollationStrength.fromInt(document.getInteger("strength")));
		}
		if (document.get("numericOrdering") != null) {
			builder.numericOrdering(document.getBoolean("numericOrdering"));
		}
		if (document.get("alternate") != null) {
			builder.collationAlternate(CollationAlternate.fromString(document.getString("alternate")));
		}
		if (document.get("maxVariable") != null) {
			builder.collationMaxVariable(CollationMaxVariable.fromString(document.getString("maxVariable")));
		}
		if (document.get("normalization") != null) {
			builder.normalization(document.getBoolean("normalization"));
		}
		if (document.get("backwards") != null) {
			builder.backwards(document.getBoolean("backwards"));
		}
		return builder.build();
	}

	public static List<Connections> getConnections(Query query, Map<String, Object> params, ClientMongoOperator clientMongoOperator, boolean needDecodePassword) {
		List<Connections> connectionsList = new ArrayList<>();
		if (clientMongoOperator != null) {
			if (query == null) {
				if (params != null) {
					Criteria criteria = new Criteria();
					params.forEach((k, v) -> criteria.and(k).is(v));
					query = new Query(criteria);
				} else {
					throw new IllegalArgumentException();
				}
			}
			query.fields().exclude("schema");
			connectionsList = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
		}

		if (CollectionUtils.isNotEmpty(connectionsList) && needDecodePassword) {
			for (Connections connections : connectionsList) {
				connections.decodeDatabasePassword();
			}
		}

		return connectionsList;
	}

	public static Connections getConnections(Query query, ClientMongoOperator clientMongoOperator, boolean needDecodePassword) {
		Connections connections;
		if (null == clientMongoOperator || null == query) {
			throw new IllegalArgumentException();
		}

		query.fields().exclude("schema");
		connections = clientMongoOperator.findOne(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);

		if (null != connections && needDecodePassword) {
			connections.decodeDatabasePassword();
		}

		return connections;
	}

	public static int getVersion(Connections connections) throws UnsupportedEncodingException {
		int versionNum = 0;
		if (connections != null && StringUtils.equalsAnyIgnoreCase(connections.getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
			MongoClient mongoClient = null;
			try {
				mongoClient = MongodbUtil.createMongoClient(connections);
				MongoDatabase mongoDatabase = mongoClient.getDatabase(MongodbUtil.getDatabase(connections));

				Document buildinfo = mongoDatabase.runCommand(new BsonDocument(BUILDINFO, new BsonString("")));

				String versionStr = buildinfo.get(VERSION).toString();

				String[] versions = versionStr.split("\\.");

				versionNum = Integer.valueOf(versions[0]);
			} finally {
				releaseConnection(mongoClient, null);
			}
		}

		return versionNum;
	}

	public static String maskUriPassword(String mongodbUri) {
		if (StringUtils.isNotBlank(mongodbUri)) {
			try {
				MongoClientURI mongoClientURI = new MongoClientURI(mongodbUri);
				MongoCredential credentials = mongoClientURI.getCredentials();
				if (credentials != null) {
					char[] password = credentials.getPassword();
					if (password != null) {
						String pass = new String(password);
						pass = URLEncoder.encode(pass, "UTF-8");

						mongodbUri = StringUtils.replaceOnce(mongodbUri, pass + "@", "******@");
					}
				}

			} catch (Exception e) {
				logger.error("Mask password for mongodb uri {} failed {}", mongodbUri, e);
			}
		}

		return mongodbUri;
	}

	public static String getFullVersion(Connections connections) throws UnsupportedEncodingException {
		String versionStr = null;
		if (connections != null && StringUtils.equalsAny(connections.getDatabase_type(),
				DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType(), DatabaseTypeEnum.GRIDFS.getType())) {

			MongoClient mongoClient = null;
			try {
				mongoClient = MongodbUtil.createMongoClient(connections);
				MongoDatabase mongoDatabase = mongoClient.getDatabase(MongodbUtil.getDatabase(connections));

				Document buildinfo = mongoDatabase.runCommand(new BsonDocument(BUILDINFO, new BsonString("")));

				versionStr = buildinfo.get(VERSION).toString();


			} finally {
				releaseConnection(mongoClient, null);
			}
		}

		return versionStr;
	}

	/**
	 * @return true: oplog
	 * false: change stream
	 */
	public static Boolean checkOplogOrChangeStream(Job job, Connections connections) throws UnsupportedEncodingException {
		Boolean isOplog = new Boolean(true);

		if (job != null && connections != null) {
			String mappingTemplate = job.getMapping_template();

			if (StringUtils.isNotBlank(mappingTemplate)) {
				int versionNum = MongodbUtil.getVersion(connections);

				if (versionNum >= MONGODB_VERSION_4) {
					job.setIs_changeStream_mode(true); // auto set, after ui add this setting, just annotation this line
					isOplog = !job.isIs_changeStream_mode();
				}
			}
		}

		return isOplog;
	}

	public static Map<String, List<DataRules>> getDataRules(ClientMongoOperator clientMongoOperator, Connections connections) {
		String rule_def = "rule_def";
		Map<String, List<DataRules>> dataRulesMap = new HashMap<>();

		if (clientMongoOperator != null && connections != null) {
			Query query = new Query(new Criteria().andOperator(
					Criteria.where("meta_type").is("collection"),
					Criteria.where("source._id").is(connections.getId())
			));
			query.fields().include("data_rules").include("original_name");

			List<Map> metadataInstances = clientMongoOperator.find(query, "MetadataInstances", Map.class);

			if (CollectionUtils.isNotEmpty(metadataInstances)) {
				for (Map metadataInstance : metadataInstances) {
					if (MapUtils.isNotEmpty(metadataInstance) && metadataInstance.containsKey("data_rules")) {

						List<DataRules> dataRules = new ArrayList<>();
						String tableName = (String) metadataInstance.get("original_name");
						Map dataRule = (Map) metadataInstance.get("data_rules");

						if (MapUtils.isNotEmpty(dataRule) && dataRule.containsKey("rules")) {
							List<Map> rules = (List) dataRule.get("rules");
							if (CollectionUtils.isNotEmpty(rules)) {
								for (Map rule : rules) {
									Map ruleDef = (Map) rule.get(rule_def);
									if (MapUtils.isNotEmpty(ruleDef)) {
										try {
											DataRules dr = new DataRules(
													(String) rule.get("field_name"),
													(String) ruleDef.get("name"),
													(String) ruleDef.get("rules"),
													ruleDef.get("status") == null ? 1 : (int) ruleDef.get("status"),
													(String) ruleDef.get("user_id")
											);
											dataRules.add(dr);
										} catch (NumberFormatException e) {
											continue;
										}
									}
								}
							}
						}

						dataRulesMap.put(tableName, dataRules);
					}
				}
			}

			return dataRulesMap;
		} else {
			return null;
		}
	}

	public static Worker getWorkerByAgentIdAndWorkType(String agentId, String workType, ClientMongoOperator clientMongoOperator) {
		if (StringUtils.isNotBlank(agentId) && StringUtils.isNotBlank(workType) && clientMongoOperator != null) {
			Criteria agentIdWhere = new Criteria("process_id").is(agentId);
			Criteria workTypeWhere = new Criteria("worker_type").is(workType);
			Query query = new Query(new Criteria().andOperator(agentIdWhere, workTypeWhere));

			Worker worker = clientMongoOperator.findOne(query, ConnectorConstant.WORKER_COLLECTION, Worker.class);
			return worker;
		} else {
			throw new RuntimeException("Params is invalid. agentId: " + agentId + ", work type: " + workType);
		}
	}

	public static List<String> getHosts(Connections connections) {
		List<String> hosts = new ArrayList<>();

		String databaseUri = connections.getDatabase_uri();

		if (StringUtils.isNotBlank(databaseUri)) {
			MongoClientURI uri = new MongoClientURI(databaseUri);
			hosts = uri.getHosts();
		}

		return hosts;
	}

	public static void releaseConnection(MongoClient client, MongoCursor cursor) {
		try {
			if (cursor != null) {
				cursor.close();
			}

			if (client != null) {
				client.close();
			}
		} catch (Exception e) {
			// abort
		}
	}

	public static void sampleDataRow(MongoCollection collection, int sampleSize, Consumer<BsonDocument> callback) {

		int sampleTime = 1;
		int sampleBatchSize = SAMPLE_SIZE_BATCH_SIZE;
		if (sampleSize > SAMPLE_SIZE_BATCH_SIZE) {
			if (sampleSize % SAMPLE_SIZE_BATCH_SIZE != 0) {
				sampleTime = sampleSize / SAMPLE_SIZE_BATCH_SIZE + 1;
			} else {
				sampleTime = sampleSize / SAMPLE_SIZE_BATCH_SIZE;
			}
		} else {
			sampleBatchSize = sampleSize;
		}
		int finalSampleBatchSize = sampleBatchSize;
		IntStream.range(0, sampleTime).forEach(i -> {
			List<Document> pipeline = new ArrayList<>();
			pipeline.add(new Document("$sample", new Document("size", finalSampleBatchSize)));
			MongoCursor<BsonDocument> cursor = null;
			try {
				cursor = collection.aggregate(pipeline).allowDiskUse(true).iterator();
				while (cursor.hasNext()) {
					BsonDocument next = cursor.next();
					callback.accept(next);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				releaseConnection(null, cursor);
			}
		});
	}

	public static long getCollectionCount(Connections connection, List<Mapping> mappings, boolean isTarget, MongoClient mongoClient) {
		long dbCount = 0l;
		String sourceDB = null;
		String databaseUri = connection.getDatabase_uri();
		if (StringUtils.isNotBlank(databaseUri)) {
			MongoClientURI uri = new MongoClientURI(databaseUri);
			sourceDB = uri.getDatabase();
		} else {
			sourceDB = connection.getDatabase_name();
		}

		if (StringUtils.isBlank(sourceDB)) {
			return dbCount;
		}
		MongoDatabase database = mongoClient.getDatabase(sourceDB);

		Set<String> statedTables = new HashSet<>(mappings.size());
		for (Mapping mapping : mappings) {
			String collectionName = null;
			if (isTarget) {
				collectionName = mapping.getTo_table();
			} else {
				collectionName = mapping.getFrom_table();
			}
			String relationship = mapping.getRelationship();

			if (ConnectorConstant.RELATIONSHIP_ONE_ONE.equals(relationship) && !statedTables.contains(collectionName)) {
				Document document = database.runCommand(new Document("collStats", collectionName));
				Object count = document.get("count");
				long collCount = 0L;
				if (count instanceof Integer) {
					collCount += new BigDecimal((Integer) count).longValue();
				} else if (count instanceof Double) {
					collCount += new BigDecimal((Double) count).longValue();
				}
				dbCount += collCount;
				statedTables.add(collectionName);
			}
		}
		return dbCount;
	}

	/**
	 * 注意，这个使用的aggregate([{group: {_id: null, n: {$sum: 1}}}])的方式进行count，当表数据量大时，会非常慢，慎用
	 * 除非对行数要求非常精确，否则建议使用使用{@link MongodbUtil#getCollectionNotAggregateCountByTableName}
	 *
	 * @param connection
	 * @param tableName
	 * @param filter
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static long getCollectionCountByTableName(Connections connection, String tableName, String filter) throws UnsupportedEncodingException {

		long dbCount = 0L;
		MongoClient mongoClient = null;
		try {
			mongoClient = MongodbUtil.createMongoClient(connection);
			String db;
			String databaseUri = connection.getDatabase_uri();
			if (StringUtils.isNotBlank(databaseUri)) {
				MongoClientURI uri = new MongoClientURI(databaseUri);
				db = uri.getDatabase();
			} else {
				db = connection.getDatabase_name();
			}

			if (StringUtils.isBlank(db)) {
				return dbCount;
			}
			MongoDatabase database = mongoClient.getDatabase(db);
			MongoCollection<Document> collection = database.getCollection(tableName);
			if (StringUtils.isNotBlank(filter)) {
				Map<String, String> filterMap = splitFilter(filter);
				if (filterMap != null) {
					if (filterMap.get("filter") != null) {
						dbCount = collection.countDocuments(Document.parse(filterMap.get("filter")));
						return dbCount;
					}
				}
			}
			dbCount = collection.countDocuments();
		} finally {
			releaseConnection(mongoClient, null);
		}
		return dbCount;
	}

	public static long getCollectionNotAggregateCountByTableName(Connections connection, String tableName, String filter) throws UnsupportedEncodingException {

		long dbCount = 0L;
		MongoClient mongoClient = null;
		try {
			mongoClient = MongodbUtil.createMongoClient(connection);
			String db;
			String databaseUri = connection.getDatabase_uri();
			if (StringUtils.isNotBlank(databaseUri)) {
				MongoClientURI uri = new MongoClientURI(databaseUri);
				db = uri.getDatabase();
			} else {
				db = connection.getDatabase_name();
			}

			if (StringUtils.isBlank(db)) {
				return dbCount;
			}
			dbCount = getCollectionNotAggregateCountByTableName(
					mongoClient,
					db,
					tableName,
					StringUtils.isNotBlank(filter) ? Document.parse(filter) : new Document()
			);
		} finally {
			releaseConnection(mongoClient, null);
		}
		return dbCount;
	}

	public static long getCollectionNotAggregateCountByTableName(MongoClient mongoClient, String db, String collectionName, Document filter) {
		long dbCount = 0L;
		MongoDatabase database = mongoClient.getDatabase(db);
		Document countDocument = database.runCommand(
				new Document("count", collectionName)
						.append("query", filter == null ? new Document() : filter)
		);

		if (countDocument.containsKey("ok") && countDocument.containsKey("n")) {
			if (countDocument.get("ok").equals(1d)) {
				dbCount = Long.valueOf(countDocument.get("n") + "");
			}
		}

		return dbCount;
	}

	public static Long count(String objectName, Connections connections) {
		return count(objectName, connections, null);
	}

	public static Long count(String objectName, Connections connections, Mapping mapping) {
		if (StringUtils.isBlank(objectName) || connections == null) {
			throw new IllegalArgumentException("Required parameters are missing, connections: " + connections + ", object name: " + objectName);
		}
		try {
			long count = MongodbUtil.getCollectionNotAggregateCountByTableName(connections, objectName, null == mapping ? "" : mapping.getDataFliter());
			return count;
		} catch (Exception e) {
			String err = "Mongodb count collection failed, connection name: " + connections.getName() + ", uri: " + MongodbUtil.maskUriPassword(connections.getDatabase_uri()) + ", collection name: " + objectName
					+ ", err: " + e.getMessage();
			throw new RuntimeException(err, e);
		}
	}

	public static Map<String, String> splitFilter(String filterStr) {
		if (StringUtils.isEmpty(filterStr)) {
			return null;
		}
		Map<String, String> filterMap = new HashMap<>();
		int filterIndex = filterStr.indexOf("({");
		int filterLastIndex = filterStr.indexOf("})");
		if (filterIndex > 0 && filterLastIndex > 0) {
			String str = filterStr.substring(filterIndex + 1, filterLastIndex + 1);
			String[] split = str.replaceAll(" ", "").split("},");
			String filter = split[0] + "}";
			filterMap.put("filter", filter);
			if (split.length > 1) {
				String projection = split[1];
				filterMap.put("projection", projection);
			}
		}
		int sortIndex = filterStr.indexOf(".sort(");
		if (sortIndex > 0) {
			String sortStr = filterStr.substring(sortIndex + 6);
			int sortLastIndex = sortStr.indexOf(")");
			if (sortLastIndex > 0) {
				filterMap.put("sort", sortStr.substring(0, sortLastIndex));
			}
		}
		int limitIndex = filterStr.indexOf(".limit(");
		if (limitIndex > 0) {
			String limitStr = filterStr.substring(limitIndex + 7);
			int limitLastIndex = limitStr.indexOf(")");
			if (limitLastIndex > 0) {
				filterMap.put("limit", limitStr.substring(0, limitLastIndex));
			}
		}
		int skipIndex = filterStr.indexOf(".skip(");
		if (skipIndex > 0) {
			String skipStr = filterStr.substring(skipIndex + 6);
			int skipLastIndex = skipStr.indexOf(")");
			if (skipLastIndex > 0) {
				filterMap.put("skip", skipStr.substring(0, skipLastIndex));
			}
		}

		return filterMap;

	}

	public static void updateCurrentDate(ClientMongoOperator clientMongoOperator, List<Object> objectIds, String collectionName, Job job) throws Exception {
		if (clientMongoOperator != null && CollectionUtils.isNotEmpty(objectIds)) {
			List<WriteModel<Document>> writeModels = new ArrayList<>();
			for (Object objectId : objectIds) {
				Document query = new Document("_id", objectId);
				Update update = new Update();
				update.currentDate(DataQualityTag.SUB_COLUMN_NAME + ".ts");

				writeModels.add(new UpdateManyModel(query, update.getUpdateObject(), new UpdateOptions().upsert(true)));
			}
			clientMongoOperator.executeBulkWrite(writeModels, new BulkWriteOptions().ordered(false), collectionName, Job.ERROR_RETRY, Job.ERROR_RETRY_INTERVAL, job);
		}
	}

	public static void insertAddObjectId(String mongodbTs, Update update, List<Object> objectIds) {
		Document updateObject = (Document) update.getUpdateObject().get("$set");
		if (updateObject.containsKey("_id")) {
			objectIds.add(updateObject.get("_id"));
		} else {
			if (mongodbTs.equals("true")) {
				ObjectId objectId = new ObjectId();
				update.set("_id", objectId);
				objectIds.add(objectId);
			}
		}
	}

	public static boolean checkIndexNameIfExists(MongoCollection collection, String indexName) {
		ListIndexesIterable listIndexesIterable = collection.listIndexes();
		for (Object o : listIndexesIterable) {
			Document doc = (Document) o;
			if (doc.get("name").equals(indexName)) {
				return true;
			}
		}

		return false;
	}

	public static int checkTargetMongodbIsActive(String uri) {
		boolean b = true;

		if (uri != null) {
			MongoClient client = null;
			try {
				client = new MongoClientProxy(new MongoClientURI(uri));

				MongoDatabase database = client.getDatabase("admin");

				Document document = database.runCommand(new Document("ping", 1));

				if (document == null
						|| !document.containsKey("ok")
						|| !document.getDouble("ok").equals(1.0)) {
					b = false;
				}
			} catch (Exception e) {
				b = false;
			} finally {
				if (client != null) {
					client.close();
				}
			}
		} else {
			b = false;
		}

		return b ? 1 : 0;
	}

	public static void statsBulkOpResult(BulkOpResult bulkOpResult, Stats stats, TapdataShareContext tapdataShareContext, boolean initial) {
		Map<String, Long> total = stats.getTotal();
		List<StageRuntimeStats> stageRuntimeStats = stats.getStageRuntimeStats();
		StageRuntimeStats stageRuntimeStat = new StageRuntimeStats();
		if (CollectionUtils.isNotEmpty(stageRuntimeStats)) {
			for (StageRuntimeStats runtimeStat : stageRuntimeStats) {
				if (runtimeStat.getStageId().equals(bulkOpResult.getStageId())) {
					stageRuntimeStat = runtimeStat;
					break;
				}
			}
		}
		BulkWriteResult result = bulkOpResult.getBulkWriteResult();
		int processedSize = bulkOpResult.getProcessSize() == null ? 0 : bulkOpResult.getProcessSize();
		long totalDataQuality = bulkOpResult.getTotalDataQuality() == null ? 0l : bulkOpResult.getTotalDataQuality();

		int insertedCount = 0;
		int deletedCount = 0;
		int modifiedCount = 0;
		List<BulkWriteUpsert> upserts = new ArrayList<>();

		if (result != null) {
			insertedCount = result.getInsertedCount();
			deletedCount = result.getDeletedCount();
			modifiedCount = result.getModifiedCount();
			upserts = result.getUpserts();
		}
		if (insertedCount > 0) {
			Long targetInserted = total.get("target_inserted");
			targetInserted += insertedCount;
			total.put("target_inserted", targetInserted);
			stageRuntimeStat.getInsert().incrementRows(insertedCount);
		}

		if (deletedCount > 0) {
			Long targetUpdated = total.get("target_updated");
			targetUpdated += deletedCount;
			total.put("target_updated", targetUpdated);

			Long totalDeleted = total.get("total_deleted");
			totalDeleted += deletedCount;
			total.put("total_deleted", totalDeleted);
			stageRuntimeStat.getDelete().incrementRows(deletedCount);
		}

		if (modifiedCount > 0) {
			Long targetUpdated = total.get("target_updated");
			targetUpdated += modifiedCount;
			total.put("target_updated", targetUpdated);

			Long totalUpdated = total.get("total_updated");
			totalUpdated += modifiedCount;
			total.put("total_updated", totalUpdated);
			stageRuntimeStat.getUpdate().incrementRows(modifiedCount);
		}

		if (CollectionUtils.isNotEmpty(upserts)) {
			int size = upserts.size();
			Long targetInserted = total.get("target_inserted");
			targetInserted += size;
			total.put("target_inserted", targetInserted);
			stageRuntimeStat.getInsert().incrementRows(size);
		}

		long processed = total.get("processed") == null ? 0l : total.get("processed");
		processed += processedSize;
		long sourceReceived = total.get("source_received") == null ? 0l : total.get("source_received");
		sourceReceived += processedSize;
		totalDataQuality += total.get(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME) == null ? 0l : total.get(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME);
		total.put("processed", processed);
		total.put("source_received", sourceReceived);
		total.put(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME, totalDataQuality);

		if (StringUtils.isNoneBlank(bulkOpResult.getTargetConnectionId())
				&& tapdataShareContext != null && CollectionUtils.isNotEmpty(tapdataShareContext.getInitialStats()) && initial) {
			tapdataShareContext.getInitialStats().stream()
					.filter(countStat -> countStat.getTargetConnectionId().equals(bulkOpResult.getTargetConnectionId())
							&& countStat.getTargetTableName().equals(bulkOpResult.getTargetCollectionName()))
					.findFirst()
					.ifPresent(cs -> cs.setTargetRowNum(cs.getTargetRowNum() + processedSize));
		}
	}

	public static Map<String, String> getHostPortMap(String uri) {
		Map<String, String> map = new HashMap<>();
		StringBuffer hostBuffer = new StringBuffer();
		StringBuffer portBuffer = new StringBuffer();

		if (StringUtils.isNotBlank(uri)) {
			MongoClientURI mongoClientURI = new MongoClientURI(uri);
			List<String> hostList = mongoClientURI.getHosts();
			if (CollectionUtils.isNotEmpty(hostList)) {
				for (String hostPort : hostList) {
					if (StringUtils.isNotBlank(hostPort)) {
						try {
							if (StringUtils.containsIgnoreCase(hostPort, ":")) {
								String[] split = hostPort.split(":");
								hostBuffer.append(split[0] + ",");
								portBuffer.append(split[1] + ",");
							} else {
								hostBuffer.append(hostPort + ",");
								portBuffer.append("27017,");
							}
						} catch (Exception e) {
							continue;
						}
					}
				}
			}
		}

		String hosts = StringUtil.removeSuffix(hostBuffer.toString(), ",");
		String ports = StringUtil.removeSuffix(portBuffer.toString(), ",");

		map.put("hosts", hosts);
		map.put("ports", ports);

		return map;
	}

	public static List<String> getConfigServerHosts(String uri) {
		List<String> list = new ArrayList<>();
		if (StringUtils.isNotBlank(uri)) {
			try (
					MongoClient mongoClient = new MongoClientProxy(new MongoClientURI(uri))
			) {
				Document document = mongoClient.getDatabase("admin").runCommand(new Document("getCmdLineOpts", 1));
				if (MapUtils.isNotEmpty(document)) {
					if (document.containsKey("parsed")) {
						Document parsed = (Document) document.get("parsed");
						if (MapUtils.isNotEmpty(parsed) && parsed.containsKey("sharding")) {
							Document sharding = (Document) parsed.get("sharding");
							if (MapUtils.isNotEmpty(sharding) && sharding.containsKey("configDB")) {
								String configs = (String) sharding.get("configDB");
								if (StringUtils.isNotBlank(configs)) {
									int i = StringUtils.indexOfIgnoreCase(configs, "/");
									if (i >= 0) {
										configs = configs.substring(i + 1);
									}
									String[] split = configs.split(",");
									for (String s : split) {
										list.add(s);
									}
								}
							}
						}
					}
				}
			}

		}

		return list;
	}

	public static String appendMongoUri(String hostPort, String username, char[] password, String serverSelectionTimeoutMS, String authSource) {
		if (StringUtils.isBlank(hostPort)) {
			return "";
		}

		String uri;

		if (StringUtils.isNotBlank(username) && password != null && password.length > 0) {
			uri = "mongodb://" + username + ":" + new String(password) + "@" + hostPort + "/admin?authSource=" + authSource + "&serverSelectionTimeoutMS=" + serverSelectionTimeoutMS;
		} else {
			uri = "mongodb://" + hostPort + "/admin?serverSelectionTimeoutMS=" + serverSelectionTimeoutMS;
		}

		return uri;
	}

	public static String appendMongoUri(String hostPort, String username, char[] password) {
		if (StringUtils.isBlank(hostPort)) {
			return "";
		}

		String uri;

		if (StringUtils.isNotBlank(username) && password != null && password.length > 0) {
			uri = "mongodb://" + username + ":" + new String(password) + "@" + hostPort;
		} else {
			uri = "mongodb://" + hostPort;
		}

		return uri;
	}

	public static String mongodbKeySpecialCharHandler(String key, String replacement) {

		if (StringUtils.isNotBlank(key)) {
			if (key.startsWith("$")) {
				key = key.replaceFirst("\\$", replacement);
			}

			if (key.contains(".") && !key.startsWith(SUB_COLUMN_NAME + ".")) {
				key = key.replaceAll("\\.", replacement);
			}

			if (key.contains(" ")) {
				key = key.replaceAll(" ", replacement);
			}
		}

		return key;
	}

	public static void constructCriteriaInTarget(List<Map<String, String>> joinCondition, Map<String, Object> value, String replacement, Criteria where) {
		if (CollectionUtils.isNotEmpty(joinCondition) && MapUtils.isNotEmpty(value) && where != null) {
			for (Map<String, String> condition : joinCondition) {
				for (Map.Entry<String, String> entry : condition.entrySet()) {
					String sourceFieldName = entry.getValue();
					try {
						Object conditionValue = MapUtil.getValueByKey(value, sourceFieldName, replacement);
						where.and(entry.getKey()).is(conditionValue);
					} catch (NullPointerException e) {
						continue;
					}
				}
			}
		}
	}

	/**
	 * 关联条件中不包含_id 且 写入路劲为空，需要将_id移除（当源库的记录_id发生变更（重新写入）时，目标mongodb的_id无法更新）
	 *
	 * @param joinCondition
	 * @param targetPath    写入目表的位置
	 * @param value
	 */
	public static void removeIdIfNeed(List<Map<String, String>> joinCondition, String targetPath, Map<String, Object> value) {
		// 写入内嵌字段时，不会发生_id冲突的问题，无需移除
		if (StringUtils.isNotBlank(targetPath)) {
			return;
		}
		if (!MongodbUtil.containIdInCondition(joinCondition)) {
			if (MapUtils.isNotEmpty(value) && value.containsKey("_id")) {
				value.remove("_id");
			}
		}
	}

	/**
	 * 判断关联条件中是否包含_id字段
	 *
	 * @param joinCondition
	 * @return
	 */
	public static boolean containIdInCondition(List<Map<String, String>> joinCondition) {
		boolean containId = false;

		if (CollectionUtils.isNotEmpty(joinCondition)) {
			for (Map<String, String> condition : joinCondition) {
				for (Map.Entry<String, String> entry : condition.entrySet()) {
					String fieldName = entry.getValue();
					if ("_id".equals(fieldName)) {
						containId = true;
						break;
					}
				}

				if (containId) {
					break;
				}
			}
		}
		return containId;
	}

	public static void constructFilterInTarget(List<Map<String, String>> joinCondition, Map<String, Object> value, String replacement, Document filter) {
		if (CollectionUtils.isNotEmpty(joinCondition) && MapUtils.isNotEmpty(value) && filter != null) {
			for (Map<String, String> condition : joinCondition) {
				for (Map.Entry<String, String> entry : condition.entrySet()) {
					String fieldName = entry.getValue();
					try {
						Object conditionValue = MapUtil.getValueByKey(value, fieldName, replacement);
						filter.append(fieldName, conditionValue);
					} catch (NullPointerException e) {
						continue;
					}
				}
			}
		}
	}

	@Override
	public boolean isIndexExists(Connections connections, String tableName, List<String> fieldNames) throws BaseDatabaseUtilException {

		if (connections != null
				&& StringUtils.equalsAnyIgnoreCase(connections.getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())
				&& CollectionUtils.isNotEmpty(fieldNames)
				&& fieldNames.size() > 0
				&& StringUtils.isNotBlank(tableName)) {

			TreeSet<String> prepareCreateIndexFields = new TreeSet<>(fieldNames);
			prepareCreateIndexFields.comparator();

			try (MongoClient mongoClient = createMongoClient(connections)) {

				MongoCollection<Document> collection = mongoClient.getDatabase(MongodbUtil.getDatabase(connections)).getCollection(tableName);

				for (Document document : collection.listIndexes()) {
					if (document.containsKey("key") && document.get("key") instanceof Document) {
						Document key = (Document) document.get("key");

						TreeSet<String> keySets = new TreeSet<>(key.keySet());
						keySets.comparator();

						if (String.join(",", prepareCreateIndexFields).equals(String.join(",", keySets))) {
							return true;
						}
					}
				}

			} catch (UnsupportedEncodingException e) {
				throw new BaseDatabaseUtilException(e);
			}

		}

		return false;
	}

	@Override
	public boolean isIndexExists(ClientMongoOperator clientMongoOperator, String tableName, List<String> fieldNames) throws BaseDatabaseUtilException {
		if (clientMongoOperator != null
				&& CollectionUtils.isNotEmpty(fieldNames)
				&& fieldNames.size() > 0
				&& StringUtils.isNotBlank(tableName)) {

			TreeSet<String> prepareCreateIndexFields = new TreeSet<>(fieldNames);
			prepareCreateIndexFields.comparator();

			try {

				MongoCollection<Document> collection = clientMongoOperator.getMongoTemplate().getDb().getCollection(tableName);

				for (Document document : collection.listIndexes()) {
					if (document.containsKey("key") && document.get("key") instanceof Document) {
						Document key = (Document) document.get("key");

						TreeSet<String> keySets = new TreeSet<>(key.keySet());
						keySets.comparator();

						if (String.join(",", prepareCreateIndexFields).equals(String.join(",", keySets))) {
							return true;
						}
					}
				}

			} catch (Exception e) {
				throw new BaseDatabaseUtilException(e);
			}

		}

		return false;
	}

	@Override
	public void dropIndex(ClientMongoOperator clientMongoOperator, String tableName, List<String> fieldNames) throws BaseDatabaseUtilException {
		if (clientMongoOperator != null
				&& StringUtils.isNotBlank(tableName)
				&& CollectionUtils.isNotEmpty(fieldNames)
				&& fieldNames.size() > 0) {

			MongoCollection<Document> collection = clientMongoOperator.getMongoTemplate().getDb().getCollection(tableName);

			Document indexByKeys = findIndexByKeys(collection, fieldNames);

			if (MapUtils.isNotEmpty(indexByKeys) && indexByKeys.containsKey("name") && indexByKeys.get("name") instanceof String) {
				collection.dropIndex((String) indexByKeys.get("name"));
			}
		}
	}

	public static void createIndex(String tableName, List<Map<String, String>> condition, ClientMongoOperator clientMongoOperator, List<TableIndex> indices) throws MongodbException {

		long count = checkCreateIndexCountLimit(tableName, clientMongoOperator);
		if (count > 0) {
			Document key = new Document();
			condition.forEach(map -> map.entrySet().stream()
					.forEachOrdered(e -> key.put(e.getKey(), 1)));

			throw new MongodbException(
					String.format(
							"Automatic index creation in target table %s skipped. Target table count %s, greater than threshold %s. Index key is: %s",
							tableName,
							NumberFormat.getInstance().format(count),
							NumberFormat.getInstance().format(ConnectorConstant.CREATE_INDEX_COUNT_LIMIT),
							key.toJson()
					));
		}

		List<String> fieldNames = new ArrayList<>();

		condition.forEach(map -> map.entrySet().stream().forEachOrdered(entry -> fieldNames.add(entry.getKey())));

		// _id index no need to create
		if (fieldNames.size() == 1 && "_id".equals(fieldNames.get(0))) {
			return;
		}
		final IndexOptions indexOptions = new IndexOptions();
		if (CollectionUtils.isNotEmpty(indices)) {
			final List<TableIndex> uniqueIndices = indices.stream().filter(TableIndex::isUnique).collect(Collectors.toList());
			if (CollectionUtils.isNotEmpty(uniqueIndices)) {
				for (TableIndex uniqueIndex : uniqueIndices) {
					final List<String> uniqueIndexCols = uniqueIndex.getColumns().stream().map(TableIndexColumn::getColumnName).collect(Collectors.toList());
					if (CollectionUtils.isEqualCollection(uniqueIndexCols, fieldNames)) {
						indexOptions.unique(true);
						break;
					}
				}
			}
		}

		if (CollectionUtils.isNotEmpty(fieldNames)
				&& !new MongodbUtil().isIndexExists(clientMongoOperator, tableName, fieldNames)) {
			List<IndexModel> indexModels = new ArrayList<>();
			StringBuilder idxNameBuilder = new StringBuilder();
			fieldNames.forEach(s -> idxNameBuilder.append(s).append("-"));
			idxNameBuilder.setLength(idxNameBuilder.length() - 1);
			String idxName = idxNameBuilder.toString();
			if (idxName.getBytes(StandardCharsets.UTF_8).length > 109) {
				idxName = UUIDGenerator.uuid();
			}
			IndexModel model = new IndexModel(Indexes.ascending(fieldNames), indexOptions.name(idxName));
			indexModels.add(model);
			try {
				clientMongoOperator.createIndexes(tableName, indexModels);
			} catch (Exception e) {
				throw new MongodbException(String.format("Create mongodb index error: %s", e.getMessage()), e);
			}
		}
	}

	public static long checkCreateIndexCountLimit(String tableName, ClientMongoOperator clientMongoOperator) {
		long count = clientMongoOperator.count(new Query(), tableName);
		return count <= ConnectorConstant.CREATE_INDEX_COUNT_LIMIT ? 0 : count;
	}

	private static Document findIndexByKeys(MongoCollection<Document> collection, List<String> fieldNames) {
		if (CollectionUtils.isNotEmpty(fieldNames)
				&& fieldNames.size() > 0
				&& collection != null) {
			TreeSet<String> prepareCreateIndexFields = new TreeSet<>(fieldNames);
			prepareCreateIndexFields.comparator();

			for (Document document : collection.listIndexes()) {
				if (document.containsKey("key") && document.get("key") instanceof Document) {
					Document key = (Document) document.get("key");

					TreeSet<String> keySets = new TreeSet<>(key.keySet());
					keySets.comparator();

					if (String.join(",", prepareCreateIndexFields).equals(String.join(",", keySets))) {
						return document;
					}
				}
			}
		}

		return null;
	}

	public static String getSimpleMongodbUri(Connections connections) {
		if (connections == null || StringUtils.isBlank(connections.getDatabase_uri())) {
			return "";
		}

		MongoClientURI mongoClientURI = new MongoClientURI(connections.getDatabase_uri());

		return getSimpleMongodbUri(mongoClientURI);
	}

	public static String getSimpleMongodbUri(MongoClientURI mongoClientURI) {
		if (mongoClientURI == null) {
			return "";
		}

		String uri = "mongodb://";

		uri += mongoClientURI.getHosts().stream().collect(Collectors.joining(",")).trim();

		if (StringUtils.isNotBlank(mongoClientURI.getDatabase())) {
			uri += "/" + mongoClientURI.getDatabase();
		}

		return uri;
	}

	public static List<Document> parsePipelineJson(String pipelineJson) {
		String originJson = pipelineJson;
		List<Document> pipelines = new LinkedList<>();

		if (StringUtils.isBlank(pipelineJson)) {
			return pipelines;
		}

		if (!pipelineJson.startsWith("[")) {
			pipelineJson = "[" + pipelineJson;
		}

		if (!pipelineJson.endsWith("]")) {
			pipelineJson = pipelineJson + "]";
		}

		pipelineJson = "{\"json\":" + pipelineJson + "}";

		Document parse;
		try {
			parse = Document.parse(pipelineJson);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Parse pipeline json error: %s. Json: %s", e.getMessage(), originJson), e);
		}

		if (parse.containsKey("json") && parse.get("json") instanceof List) {
			pipelines = (List<Document>) parse.get("json");
		}

		return pipelines;
	}

	public static String findOneManyStageById(Mapping mapping) {
		if (mapping == null) {
			return null;
		}

		final List<Stage> stages = mapping.getStages();
		if (CollectionUtils.isNotEmpty(stages)) {
			for (Stage stage : stages) {
				if (CollectionUtils.isEmpty(stage.getOutputLanes())) {
					continue;
				}
				List<String> outputLanes = stage.getOutputLanes();
				for (String outputLane : outputLanes) {
					if (StringUtils.endsWith(outputLane, ConnectorConstant.LOOKUP_TABLE_SUFFIX)) {
						return outputLane;
					}
				}
			}
		}
		return null;
	}

	public static Document recursiveEmbeddedMap(Document data) {
		if (MapUtils.isEmpty(data)) {
			return data;
		}

		Document newMap = new Document();
		data.forEach((key, value) -> {
			recursiveCreateMap(newMap, value, key);
		});

		return newMap;
	}

	private static void recursiveCreateMap(Document newData, Object value, String key) {
		if (key.contains(".")) {
			final String[] split = key.split("\\.", 2);
			if (!newData.containsKey(split[0])) {
				newData.put(split[0], new Document());
			}
			recursiveCreateMap((Document) newData.get(split[0]), value, split[1]);
		} else {
			newData.put(key, value);
		}
	}

	public static MongoClientURI verifyMongoDBUri(String uri) {
		if (StringUtils.isBlank(uri)) throw new IllegalArgumentException("MongoDB client uri is blank");
		MongoClientURI mongoClientURI;
		try {
			mongoClientURI = new MongoClientURI(uri);
		} catch (Exception e) {
			throw new IllegalArgumentException("Illegal MongoDB client uri: " + uri + ", error message: " + e.getMessage(), e);
		}
		return mongoClientURI;
	}

	public static MongoClientURI verifyMongoDBUriWithDB(String uri) {
		MongoClientURI mongoClientURI = verifyMongoDBUri(uri);
		String database = mongoClientURI.getDatabase();
		if (StringUtils.isBlank(database))
			throw new IllegalArgumentException("MongoDB client uri missing database: " + maskUriPassword(uri));
		return mongoClientURI;
	}
}

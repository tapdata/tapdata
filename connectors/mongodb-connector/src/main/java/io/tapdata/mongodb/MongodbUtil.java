package io.tapdata.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.*;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.mongodb.codecs.TapdataBigDecimalCodec;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.util.SSLUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author jackin
 * @date 2022/5/17 20:40
 **/
public class MongodbUtil {

	public final static String SUB_COLUMN_NAME = "__tapd8";

	private static final int SAMPLE_SIZE_BATCH_SIZE = 1000;

	private final static String BUILDINFO = "buildinfo";
	private final static String VERSION = "version";

	public static int getVersion(MongoClient mongoClient, String database) {
		int versionNum = 0;
		MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		Document buildinfo = mongoDatabase.runCommand(new BsonDocument(BUILDINFO, new BsonString("")));
		String versionStr = buildinfo.get(VERSION).toString();
		String[] versions = versionStr.split("\\.");
		versionNum = Integer.valueOf(versions[0]);

		return versionNum;
	}

	public static String getVersionString(MongoClient mongoClient, String database) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		Document buildinfo = mongoDatabase.runCommand(new BsonDocument(BUILDINFO, new BsonString("")));
		return buildinfo.get(VERSION).toString();
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
			try (MongoCursor<BsonDocument> cursor = collection.aggregate(pipeline, BsonDocument.class).allowDiskUse(true).iterator()) {
				while (cursor.hasNext()) {
					BsonDocument next = cursor.next();
					callback.accept(next);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	public static Map<String, String> nodesURI(MongoClient mongoClient, String mongodbURI) {
		Map<String, String> nodeConnURIs = new HashMap<>();
		ConnectionString connectionString = new ConnectionString(mongodbURI);
		String username = connectionString.getUsername();
		String password = connectionString.getPassword() != null && connectionString.getPassword().length > 0 ? new String(connectionString.getPassword()) : null;
		final String database = connectionString.getDatabase();
		final String mongoDBURIOptions = getMongoDBURIOptions(mongodbURI);
		MongoCollection<Document> collection = mongoClient.getDatabase("config").getCollection("shards");
		final MongoCursor<Document> cursor = collection.find().iterator();
		while (cursor.hasNext()) {
			Document doc = cursor.next();
			String hostStr = doc.getString("host");
			String replicaSetName = replicaSetUsedIn(hostStr);
			String addresses = hostStr.split("/")[1];
			StringBuilder sb = new StringBuilder();
			if (EmptyKit.isNotEmpty(username) && EmptyKit.isNotEmpty(password)) {
				try {
					sb.append("mongodb://").append(URLEncoder.encode(connectionString.getUsername(), "UTF-8")).append(":").append(URLEncoder.encode(String.valueOf(password), "UTF-8")).append("@").append(addresses).append("/").append(database);
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException("url encode username/password failed", e);
				}
			} else {
				sb.append("mongodb://").append(addresses).append("/").append(database);
			}
			if (EmptyKit.isNotBlank(mongoDBURIOptions)) {
				sb.append("?").append(mongoDBURIOptions);
			}
			nodeConnURIs.put(replicaSetName, sb.toString());
		}

		if (nodeConnURIs.size() == 0) {
			// The addresses may be a replica set ...
			try {
				Document document = mongoClient.getDatabase("admin").runCommand(new Document("replSetGetStatus", 1));
				List members = document.get("members", List.class);
				if (members != null && !members.isEmpty()) {

					StringBuilder sb = new StringBuilder();
					// This is a replica set ...
					for (Object member : members) {
						Document doc = (Document) member;
						sb.append(doc.getString("name")).append(",");
					}
					String addressStr = sb.deleteCharAt(sb.length() - 1).toString();
					String replicaSetName = document.getString("set");

					StringBuilder uriSB = new StringBuilder();
					if (EmptyKit.isNotBlank(username) && EmptyKit.isNotBlank(password)) {
						uriSB.append("mongodb://").append(URLEncoder.encode(username, "UTF-8")).append(":").append(URLEncoder.encode(password, "UTF-8")).append("@").append(addressStr).append("/").append(database);
					} else {
						uriSB.append("mongodb://").append(addressStr).append("/").append(database);
					}
					if (EmptyKit.isNotBlank(mongoDBURIOptions)) {
						uriSB.append("?").append(mongoDBURIOptions);
					}
					nodeConnURIs.put(replicaSetName, uriSB.toString());
				}
			} catch (Exception e) {
				String replicaSetName = "single";
				if (replicaSetName != null) {

					for (String address : connectionString.getHosts()) {
						StringBuilder sb = new StringBuilder();
						if (EmptyKit.isNotBlank(username) && EmptyKit.isNotBlank(password)) {
							try {
								sb.append("mongodb://").append(URLEncoder.encode(username, "UTF-8")).append(":").append(URLEncoder.encode(password, "UTF-8")).append("@").append(address).append("/").append(database);
							} catch (UnsupportedEncodingException ex) {
								throw new RuntimeException("url encode username/password failed", e);
							}
						} else {
							sb.append("mongodb://").append(address).append("/").append(database);
						}
						if (EmptyKit.isNotBlank(mongoDBURIOptions)) {
							sb.append("?").append(mongoDBURIOptions);
						}
						nodeConnURIs.put(replicaSetName, sb.toString());
					}
				}
			}
		}

		return nodeConnURIs;
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

	public static String getMongoDBURIOptions(String databaseUri) {
		String options = null;
		try {

			if (EmptyKit.isNotBlank(databaseUri)) {
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

	public static String maskUriPassword(String mongodbUri) {
		if (EmptyKit.isNotBlank(mongodbUri)) {
			try {
				ConnectionString connectionString = new ConnectionString(mongodbUri);
				MongoCredential credentials = connectionString.getCredential();
				if (credentials != null) {
					char[] password = credentials.getPassword();
					if (password != null) {
						String pass = new String(password);
						pass = URLEncoder.encode(pass, "UTF-8");

						mongodbUri = StringKit.replaceOnce(mongodbUri, pass + "@", "******@");
					}
				}

			} catch (Exception e) {
				TapLogger.error(MongodbUtil.class.getSimpleName(), "Mask password for mongodb uri {} failed {}", mongodbUri, e);
			}
		}

		return mongodbUri;
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

	public static String mongodbKeySpecialCharHandler(String key, String replacement) {

		if (EmptyKit.isNotBlank(key)) {
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

	public static MongoClient createMongoClient(MongodbConfig mongodbConfig) {
		CodecRegistry defaultCodecRegistry = MongoClientSettings.getDefaultCodecRegistry();
		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new TapdataBigDecimalCodec()), defaultCodecRegistry);
		final MongoClientSettings.Builder builder = MongoClientSettings.builder().codecRegistry(codecRegistry);
		String mongodbUri = mongodbConfig.getUri();
		if (null == mongodbUri || "".equals(mongodbUri)) {
			throw new RuntimeException("Create MongoDB client failed, error: uri is blank");
		}
		builder.applyConnectionString(new ConnectionString(mongodbUri));

		if (mongodbConfig.isSsl()) {
			if (EmptyKit.isNotEmpty(mongodbUri) &&
					(mongodbUri.indexOf("tlsAllowInvalidCertificates=true") > 0 ||
							mongodbUri.indexOf("sslAllowInvalidCertificates=true") > 0)) {
				builder.applyToSslSettings(sslSettingBuilder -> {
					SSLContext sslContext = null;
					try {
						sslContext = SSLContext.getInstance("SSL");
					} catch (NoSuchAlgorithmException e) {
						throw new RuntimeException(String.format("Create ssl context failed %s", e.getMessage()), e);
					}
					try {
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
					} catch (KeyManagementException e) {
						throw new RuntimeException(String.format("Initialize ssl context failed %s", e.getMessage()), e);
					}
					sslSettingBuilder.enabled(true).context(sslContext).invalidHostNameAllowed(true);
				});

			} else {
				sslMongoClientOption(mongodbConfig.isSslValidate(), mongodbConfig.getSslCA(), mongodbConfig.getSslCA(),
						mongodbConfig.getSslKey(), mongodbConfig.getSslPass(), mongodbConfig.getCheckServerIdentity(), builder);
			}
		}

		return MongoClients.create(builder.build());
	}

	public static void sslMongoClientOption(boolean sslValidate, String sslCA, String sslCert, String sslKeyStr, String sslPass,
											boolean checkServerIdentity, MongoClientSettings.Builder builder) {


		List<String> certificates = SSLUtil.retriveCertificates(sslCert);
		String sslKey = SSLUtil.retrivePrivateKey(sslKeyStr);
		if (EmptyKit.isNotBlank(sslKey) && CollectionUtils.isNotEmpty(certificates)) {

			builder.applyToSslSettings(sslSettingsBuilder -> {
				List<String> trustCertificates = null;
				if (sslValidate) {
					trustCertificates = SSLUtil.retriveCertificates(sslCA);
				}
				SSLContext sslContext = null;
				try {
					sslContext = SSLUtil.createSSLContext(sslKey, certificates, trustCertificates, sslPass);
				} catch (Exception e) {
					throw new RuntimeException(String.format("Create ssl context failed %s", e.getMessage()), e);
				}
				sslSettingsBuilder.context(sslContext);
				sslSettingsBuilder.enabled(true);
				sslSettingsBuilder.invalidHostNameAllowed(!checkServerIdentity);
			});
		}
	}

	public static String getSimpleMongodbUri(ConnectionString connectionString) {
		if (connectionString == null) {
			return "";
		}

		String uri = "mongodb://";

		uri += connectionString.getHosts().stream().collect(Collectors.joining(",")).trim();

		if (EmptyKit.isNotBlank(connectionString.getDatabase())) {
			uri += "/" + connectionString.getDatabase();
		}

		return uri;
	}

	/**
	 * If the association condition does not contain the _id and the write circuit is empty, the _id needs to be removed.
	 * When the record _id of the source database is changed (written again), the target mongodb _id cannot be updated.
	 *
	 * @param pks
	 * @param value
	 */
	public static void removeIdIfNeed(Collection<String> pks, Map<String, Object> value) {
		if (EmptyKit.isEmpty(pks)) {
			return;
		}
		if (!pks.contains("_id")) {
			if (EmptyKit.isNotEmpty(value) && value.containsKey("_id")) {
				value.remove("_id");
			}
		}
	}

	/**
	 * If the association condition does not contain the _id and the write circuit is empty,
	 * the _id needs to be removed.
	 * (When the record _id of the source database is changed (written again), the target mongodb _id cannot be updated.)
	 *
	 * @param joinCondition
	 * @param targetPath    Position to write to the catalog
	 * @param value
	 */
	public static void removeIdIfNeed(List<Map<String, String>> joinCondition, String targetPath, Map<String, Object> value) {
		// 写入内嵌字段时，不会发生_id冲突的问题，无需移除
		if (EmptyKit.isNotBlank(targetPath)) {
			return;
		}
		if (!MongodbUtil.containIdInCondition(joinCondition)) {
			if (EmptyKit.isNotEmpty(value) && value.containsKey("_id")) {
				value.remove("_id");
			}
		}
	}

	/**
	 * Check whether the association condition contains the _id field
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

	/**
	 * @param commandStr: db.test.find({}), db.getCollection('test').find({})
	 * @return
	 */
	public static Map<String, String> parseCommand(String commandStr) {
		try {
			Map<String, String> map = new HashMap<>();

			commandStr = commandStr.replaceFirst("db.", "");
			String[] split = commandStr.split("\\.", 2);
			String collection = split[0].trim();
			if (collection.startsWith("getCollection(")) {
				collection = collection.replaceFirst("getCollection\\(", "");
				collection = collection.substring(0, collection.length() - 1);
				collection = collection.replaceAll("\"", "");
				collection = collection.replaceAll("'", "");
			}
			map.put("collection", collection);
			//find({}).limit(1)
			String command = split[1].substring(0, split[1].indexOf("("));
			map.put("command", command);
			String others = split[1].substring(command.length());
			if ("find".equals(command)) {
				Map<String, String> filterMap = splitFilter(others);
				if (filterMap != null) {
					map.putAll(filterMap);
				}
			} else if ("aggregate".equals(command)) {
				//
				String pipeline = others.substring(1, others.indexOf(")"));
				map.put("pipeline", pipeline);
			} else {
				throw new IllegalArgumentException("Unsupported command: " + command);
			}

			return map;
		} catch (Throwable e) {
			throw new IllegalArgumentException("syntax command error: " + commandStr, e);
		}
	}

	public static Map<String, String> splitFilter(String filterStr) {
		if (EmptyKit.isEmpty(filterStr)) {
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

	public static void main(String[] args) {
		String command = "db.test.find({'aa':1}).limit(1)";
		Map<String, String> map = splitFilter(command);
		System.out.println(map);

//		System.out.println(getCollection("db.test.find({'aa':1}).limit(1)"));
//		System.out.println(getCollection("db.getCollection('test').find({})"));
//		System.out.println(getCollection("db.getCollection(\"test\").find({})"));

		System.out.println();
		String[] split = "getCollection('test').find({}).limit(1)".split("\\.", 2);
		for (String s : split) {
			System.out.println(s);
		}

		//find({}).limit(1)
		String substring = split[1].substring(0, split[1].indexOf("("));
		System.out.println(substring);
		System.out.println(split[1].substring(substring.length()));

		System.out.println(split[1].substring(substring.length() + 1, split[1].indexOf(")")));

		System.out.println(parseCommand("db.test.find({'aa':1}).limit(1)"));
		System.out.println(parseCommand("db.test.aggregate([{'aa':1}])"));


	}
}

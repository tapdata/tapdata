package com.tapdata.tm.mcp.mongodb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mongodb.ConnectionString;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.connection.ConnectionPoolSettings;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.mcp.exception.McpException;
import com.tapdata.tm.utils.TrustAllX509TrustManager;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import javax.net.ssl.*;
import java.io.Closeable;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.tapdata.tm.mcp.Utils.toJson;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/27 07:31
 */
public class MongoOperator implements Closeable {

    private DataSourceConnectionDto datasourceDto;
    private MongoClient mongoClient;
    private String database;

    public MongoOperator(DataSourceConnectionDto datasourceDto) {
        this.datasourceDto = datasourceDto;
    }

    private MongoDatabase getDatabase() {
        return mongoClient.getDatabase(database);
    }

    public List<Object> listCollections(boolean nameOnly) {

        List<Object> result = new ArrayList<>();
        if (nameOnly) {
            getDatabase().listCollectionNames().forEach(result::add);
        } else {
            getDatabase().listCollections().forEach(result::add);
        }
        return result;
    }

    public long count(String collectionName, Map<String, Object> params) {

        MongoCollection<Document> collection = getDatabase().getCollection(collectionName);

        Object tmp = params.get("query");
        Document query = null;
        if ( tmp != null) {
            if (tmp instanceof String) {
                query = Document.parse(tmp.toString());
            } else if (tmp instanceof Map) {
                query = new Document((Map<String, Object>) tmp);
            } else {
                throw new McpException("Parameter query must be a plain object");
            }
            convertObjectId(query);
        }

        if (query == null) {
            return collection.countDocuments();
        }

        var limit = Utils.getIntegerValue(params, "limit");
        var skip = Utils.getIntegerValue(params, "skip");
        var maxTimeMS = Utils.getLongValue(params, "maxTimeMS");

        var countOptions = new CountOptions();
        if (limit != null) countOptions.limit(limit);
        if (skip != null) countOptions.skip(skip);
        if (maxTimeMS != null) countOptions.maxTime(maxTimeMS, TimeUnit.MILLISECONDS);
        tmp = params.get("collation");
        if (tmp != null) {
            Collation collation = null;
            if (tmp instanceof Document doc && doc.containsKey("locale")) {
                collation = Collation.builder().locale(StringUtils.defaultIfBlank(doc.getString("locale"), "en")).build();
            } else if (tmp instanceof Map map && map.containsKey("locale")) {
                collation = Collation.builder().locale(map.getOrDefault("locale", "en").toString()).build();
            }
            if (collation != null)
                countOptions.collation(collation);
        }
        tmp = params.get("hint");
        if (tmp != null) {
            Document hint = null;
            if (tmp instanceof String) {
                hint = Document.parse(tmp.toString());
            } else if (tmp instanceof Map) {
                hint = new Document((Map<String, Object>) tmp);
            }
            if (hint != null)
                countOptions.hint(hint);
        }

        return collection.countDocuments(query, countOptions);
    }

    public List<Document> query(String collectionName, Map<String, Object> params) {

        MongoCollection<Document> collection = getDatabase().getCollection(collectionName);

        Object tmp = params.get("filter");
        Document filter = null;
        if ( tmp != null) {
            if (tmp instanceof String) {
                filter = Document.parse(tmp.toString());
            } else if (tmp instanceof Map) {
                filter = new Document((Map<String, Object>) tmp);
            } else {
                throw new McpException("Parameter filter must be a plain object");
            }
            convertObjectId(filter);
        }

        Document projection = null;
        tmp = params.get("projection");
        if ( tmp != null) {
            if (tmp instanceof String) {
                projection = Document.parse(tmp.toString());
            } else if (tmp instanceof Map) {
                projection = new Document((Map<String, Object>) tmp);
            } else {
                throw new McpException("Parameter projection must be a plain object");
            }
            convertObjectId(filter);
        }

        var limit = Utils.getIntegerValue(params, "limit");
        var skip = Utils.getIntegerValue(params, "skip");
        var maxTimeMS = Utils.getLongValue(params, "maxTimeMS");
        var explain = Utils.getStringValue(params, "explain");

        FindIterable<Document> findIterable = filter == null ? collection.find() : collection.find(filter);
        if (projection != null) findIterable.projection(projection);
        findIterable.limit(limit != null ? limit : 100);
        if (skip != null) findIterable.skip(skip);
        if (maxTimeMS != null) findIterable.maxTime(maxTimeMS, TimeUnit.MILLISECONDS);

        if (StringUtils.isNotBlank(explain)) {
            return Collections.singletonList(findIterable.explain(parseExplainVerbosity(explain)));
        } else {
            List<Document> result = new ArrayList<>();
            try (MongoCursor<Document> cursor = findIterable.cursor()) {
                cursor.forEachRemaining(result::add);
            }
            return result;
        }
    }

    public List<Document> aggregate(String collectionName, Map<String, Object> params) {

        Object tmp = params.get("pipeline");
        List<Document> pipeline = null;
        if (tmp instanceof List) {
            pipeline = Utils.parseJson(toJson(tmp), new TypeReference<List<Document>>() {
            });
        } else if (tmp instanceof String) {
            pipeline = Utils.parseJson(tmp.toString(), new TypeReference<List<Document>>() {
            });
        }
        if (pipeline == null)
            throw new McpException("Pipeline must be an array");
        processPipeline(pipeline);

        var explain = Utils.getStringValue(params, "explain");

        MongoCollection<Document> collection = getDatabase().getCollection(collectionName);

        AggregateIterable<Document> iterable = collection.aggregate(pipeline);
        if (StringUtils.isNotBlank(explain))
            return Collections.singletonList(iterable.explain(parseExplainVerbosity(explain)));

        List<Document> result = new ArrayList<>();
        try (MongoCursor<Document> cursor = iterable.cursor()) {
            cursor.forEachRemaining(result::add);
        }
        return result;
    }

    private void processPipeline(List<Document> pipeline) {
        for (Document document : pipeline) {
            if (document.containsKey("$group") && document.get("$group") instanceof Map) {
                Map doc = (Map) document.get("$group");
                doc.computeIfAbsent("_id", k -> new HashMap<>());
            }
        }
    }

    public void connect(){
        Map<String, Object> config = datasourceDto.getConfig();
        try {
            mongoClient = createClient(config);
        } catch (Exception e) {
            throw new McpException("Connect MongoDB failed " + e.getMessage());
        }
    }

    private MongoClient createClient(Map<String, Object> config) {
        var writeConcern = Utils.getStringValue(config, "writeConcern", "w1");
        boolean ssl = Boolean.TRUE.equals(config.get("ssl"));

        final MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .writeConcern(WriteConcern.valueOf(writeConcern));
        var uri = getConnectionString(config);
        if (null == uri || "".equals(uri)) {
            throw new McpException("Create MongoDB client failed, error: uri is blank");
        }

        ConnectionPoolSettings.Builder connectionPoolSettingsBuilder = ConnectionPoolSettings.builder();
        ConnectionPoolSettings connectionPoolSettings = connectionPoolSettingsBuilder.build();
        builder.applyToConnectionPoolSettings(settingBuilder -> {
            settingBuilder.applySettings(connectionPoolSettings);
        });

        ConnectionString connectionString = new ConnectionString(uri);
        database = connectionString.getDatabase();
        builder.applyConnectionString(connectionString);

        if (ssl) {
            if (uri.contains("tlsAllowInvalidCertificates=true") || uri.contains("sslAllowInvalidCertificates=true")) {
                builder.applyToSslSettings(sslSettingBuilder -> {
                    SSLContext sslContext = null;
                    try {
                        sslContext = SSLContext.getInstance("SSL");
                    } catch (NoSuchAlgorithmException e) {
                        throw new McpException(String.format("Create ssl context failed %s", e.getMessage()), e);
                    }
                    try {
                        sslContext.init(null, new TrustManager[]{ new TrustAllX509TrustManager()}, new SecureRandom());
                    } catch (KeyManagementException e) {
                        throw new McpException(String.format("Initialize ssl context failed %s", e.getMessage()), e);
                    }
                    sslSettingBuilder.enabled(true).context(sslContext).invalidHostNameAllowed(true);
                });

            } else {
                boolean sslValidate = Boolean.TRUE.equals(config.get("sslValidate"));
                var sslCa = Utils.getStringValue(config, "sslCA");
                var sslKey = Utils.getStringValue(config, "sslKey");
                var sslPass = Utils.getStringValue(config, "sslPass");
                boolean checkServerIdentity = Boolean.TRUE.equals(config.get("checkServerIdentity"));

                List<String> clientCertificates = SSLUtil.retrieveCertificates(sslKey);
                String clientPrivateKey = SSLUtil.retrievePrivateKey(sslKey);
                if (StringUtils.isNotBlank(clientPrivateKey) && CollectionUtils.isNotEmpty(clientCertificates)) {

                    builder.applyToSslSettings(sslSettingsBuilder -> {
                        List<String> trustCertificates = null;
                        if (sslValidate) {
                            trustCertificates = SSLUtil.retrieveCertificates(sslCa);
                        }
                        SSLContext sslContext = null;
                        try {
                            sslContext = SSLUtil.createSSLContext(clientPrivateKey, clientCertificates, trustCertificates, sslPass);
                        } catch (Exception e) {
                            throw new McpException(String.format("Create ssl context failed %s", e.getMessage()), e);
                        }
                        sslSettingsBuilder.context(sslContext);
                        sslSettingsBuilder.enabled(true);
                        sslSettingsBuilder.invalidHostNameAllowed(!checkServerIdentity);
                    });
                }
            }
        }

        return MongoClients.create(builder.build());
    }

    private String getConnectionString(Map<String, Object> config) {
        boolean isUri = Boolean.TRUE.equals(config.get("isUri"));
        if (isUri) {
            return config.get("uri").toString();
        } else {
            var host = Utils.getStringValue(config, "host");
            var database = Utils.getStringValue(config, "database");
            var user = Utils.getStringValue(config, "user");
            var additionalString = Utils.getStringValue(config, "additionalString");
            var password = Utils.getStringValue(config, "password");

            if (StringUtils.isNotBlank(user)) {
                return String.format("mongodb://%s:%s@%s/%s?%s", user, password, host, database, additionalString);
            } else {
                return String.format("mongodb://%s/%s?%s", host, database, additionalString);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (mongoClient != null)
            mongoClient.close();
    }

    private Pattern objectIdMatch = Pattern.compile("^[a-z0-9]{24}$");
    private void convertObjectId(Map<String, Object> map) {
        if (map == null)
            return;
        map.keySet().forEach(k -> {
            Object val = map.get(k);
            if ("_id".equals(k) && val instanceof String && objectIdMatch.matcher(val.toString()).matches()) {
                map.put(k, new ObjectId(val.toString()));
            }
        });
    }

    private ExplainVerbosity parseExplainVerbosity(String explain) {
        switch (explain) {
            case "executionStats":
                return ExplainVerbosity.EXECUTION_STATS;
            case "allPlansExecution":
                return ExplainVerbosity.ALL_PLANS_EXECUTIONS;
            default:
                return ExplainVerbosity.QUERY_PLANNER;
        }
    }
}

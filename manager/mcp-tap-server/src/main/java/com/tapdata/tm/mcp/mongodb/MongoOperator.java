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

    public List<?> listCollections(boolean nameOnly) {

        MongoDatabase db = mongoClient.getDatabase(database);
        List<Object> result = new ArrayList<>();
        if (nameOnly) {
            db.listCollectionNames().forEach(result::add);
        } else {
            db.listCollections().forEach(result::add);
        }
        return result;
    }

    public long count(String collectionName, Map<String, Object> params) {

        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(collectionName);

        Object tmp = params.get("query");
        Document query = null;
        if ( tmp != null) {
            if (tmp instanceof String) {
                query = Document.parse(tmp.toString());
            } else if (tmp instanceof Map) {
                query = new Document((Map<String, Object>) tmp);
            } else {
                throw new RuntimeException("Parameter query must be a plain object");
            }
            convertObjectId(query);
        }

        if (query == null) {
            return collection.countDocuments();
        }

        Integer limit = Utils.getIntegerValue(params, "limit");
        Integer skip = Utils.getIntegerValue(params, "skip");
        Long maxTimeMS = Utils.getLongValue(params, "maxTimeMS");

        CountOptions countOptions = new CountOptions();
        if (limit != null) countOptions.limit(limit);
        if (skip != null) countOptions.skip(skip);
        if (maxTimeMS != null) countOptions.maxTime(maxTimeMS, TimeUnit.MILLISECONDS);
        tmp = params.get("collation");
        if (tmp != null) {
            Collation collation = null;
            if (tmp instanceof String) {
                collation = Utils.parseJson(tmp.toString(), Collation.class);
            } else if (tmp instanceof Map) {
                collation = Utils.parseJson(toJson(tmp), Collation.class);
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

        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(collectionName);

        Object tmp = params.get("filter");
        Document filter = null;
        if ( tmp != null) {
            if (tmp instanceof String) {
                filter = Document.parse(tmp.toString());
            } else if (tmp instanceof Map) {
                filter = new Document((Map<String, Object>) tmp);
            } else {
                throw new RuntimeException("Parameter filter must be a plain object");
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
                throw new RuntimeException("Parameter projection must be a plain object");
            }
            convertObjectId(filter);
        }

        Integer limit = Utils.getIntegerValue(params, "limit");
        Integer skip = Utils.getIntegerValue(params, "skip");
        Long maxTimeMS = Utils.getLongValue(params, "maxTimeMS");
        String explain = Utils.getStringValue(params, "explain");

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
            throw new RuntimeException("Pipeline must be an array");
        processPipeline(pipeline);

        String explain = Utils.getStringValue(params, "explain");

        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(collectionName);

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
            throw new RuntimeException("Connect MongoDB failed " + e.getMessage());
        }
    }

    private MongoClient createClient(Map<String, Object> config) {
        String writeConcern = Utils.getStringValue(config, "writeConcern", "w1");
        boolean ssl = Boolean.TRUE.equals(config.get("ssl"));

        final MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .writeConcern(WriteConcern.valueOf(writeConcern));
        String uri = getConnectionString(config);
        if (null == uri || "".equals(uri)) {
            throw new RuntimeException("Create MongoDB client failed, error: uri is blank");
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
                boolean sslValidate = Boolean.TRUE.equals(config.get("sslValidate"));
                String sslCa = Utils.getStringValue(config, "sslCA");
                String sslKey = Utils.getStringValue(config, "sslKey");
                String sslPass = Utils.getStringValue(config, "sslPass");
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
                            throw new RuntimeException(String.format("Create ssl context failed %s", e.getMessage()), e);
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
        String uri = null;
        if (isUri) {
            return config.get("uri").toString();
        } else {
            String host = Utils.getStringValue(config, "host");
            String database = Utils.getStringValue(config, "database");
            String user = Utils.getStringValue(config, "user");
            String additionalString = Utils.getStringValue(config, "additionalString");
            String password = Utils.getStringValue(config, "password");

            if (StringUtils.isNotBlank(user)) {
                return String.format("mongodb://%s:%s@%s:%s/%s?%s", user, password, host, database, uri, additionalString);
            } else {
                return String.format("mongodb://%s:%s/%s?%s", host, database, uri, additionalString);
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

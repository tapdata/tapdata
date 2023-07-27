package io.tapdata.mongodb;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapdata.common.CommonDbTest;
import io.tapdata.constant.DbTestItem;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static io.tapdata.base.ConnectorBase.testItem;

public class MongodbTest extends CommonDbTest {

    protected final MongodbConfig mongodbConfig;
    protected final MongoClient mongoClient;

    protected static final Set<String> READ_PRIVILEGE_ACTIONS = new HashSet<>();
    private static final Set<String> READ_WRITE_PRIVILEGE_ACTIONS = new HashSet<>();

    private static final String CONFIG_DATABASE_SHARDS_COLLECTION = "config.shards";
    private static final String LOCAL_DATABASEOPLOG_COLLECTION = "local.oplog.rs";
    private static final String CONFIG_DATABASE = "config";
    private static final String LOCAL_DATABASE = "local";
    private static final String ADMIN_DATABASE = "admin";

    static {

        READ_PRIVILEGE_ACTIONS.add("collStats");
        READ_PRIVILEGE_ACTIONS.add("dbStats");
        READ_PRIVILEGE_ACTIONS.add("find");
        READ_PRIVILEGE_ACTIONS.add("killCursors");
        READ_PRIVILEGE_ACTIONS.add("listCollections");
        READ_PRIVILEGE_ACTIONS.add("listIndexes");

        READ_WRITE_PRIVILEGE_ACTIONS.add("collStats");
        READ_WRITE_PRIVILEGE_ACTIONS.add("dbStats");
        READ_WRITE_PRIVILEGE_ACTIONS.add("createCollection");
        READ_WRITE_PRIVILEGE_ACTIONS.add("createIndex");
        READ_WRITE_PRIVILEGE_ACTIONS.add("dropCollection");
        READ_WRITE_PRIVILEGE_ACTIONS.add("dropIndex");
        READ_WRITE_PRIVILEGE_ACTIONS.add("find");
        READ_WRITE_PRIVILEGE_ACTIONS.add("insert");
        READ_WRITE_PRIVILEGE_ACTIONS.add("killCursors");
        READ_WRITE_PRIVILEGE_ACTIONS.add("listCollections");
        READ_WRITE_PRIVILEGE_ACTIONS.add("listIndexes");
        READ_WRITE_PRIVILEGE_ACTIONS.add("remove");
        READ_WRITE_PRIVILEGE_ACTIONS.add("renameCollectionSameDB");
        READ_WRITE_PRIVILEGE_ACTIONS.add("update");
    }

    public MongodbTest(MongodbConfig mongodbConfig, Consumer<TestItem> consumer, MongoClient mongoClient) {
        super(mongodbConfig, consumer);
        this.mongodbConfig = mongodbConfig;
        this.mongoClient = mongoClient;
    }

    @Override
    protected List<String> supportVersions() {
        return Arrays.asList("3.2.*", "3.4.*", "3.6.*", "4.0.*", "4.2.*","4.4.*","5.0.*");
    }

    @Override
    public Boolean testHostPort() {
        StringBuilder failHosts;
        List<String> hosts = mongodbConfig.getHosts();
        failHosts = new StringBuilder();
        for (String host : hosts) {
            String[] split = host.split(":");
            String hostname = split[0];
            int port = 27017;
            if (split.length > 1) {
                port = Integer.parseInt(split[1]);
            }
            try {
                NetUtil.validateHostPortWithSocket(hostname, port);
            } catch (Exception e) {
                failHosts.append(host).append(",");
            }
        }
        if (EmptyKit.isNotBlank(String.valueOf(failHosts))) {
            failHosts = new StringBuilder(failHosts.substring(0, failHosts.length() - 1));
            consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, JSONObject.toJSONString(failHosts)));
            return false;
        }
        consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    @Override
    public Boolean testConnect() {
        try {
            MongoDatabase mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
            try (final MongoCursor<String> mongoCursor = mongoDatabase.listCollectionNames().iterator()) {
                mongoCursor.hasNext();
            }
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, TEST_CONNECTION_LOGIN));
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    @Override
    public Boolean testVersion() {
        try {
            String version = MongodbUtil.getVersionString(mongoClient, mongodbConfig.getDatabase());
            String versionMsg = "mongodb version: " + version;
            if (supportVersions().stream().noneMatch(v -> {
                String reg = v.replaceAll("\\*", ".*");
                Pattern pattern = Pattern.compile(reg);
                return pattern.matcher(version).matches();
            })) {
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, versionMsg + " not supported well"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, versionMsg));
            }
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    @Override
    public Boolean testReadPrivilege() {
        if (!isOpenAuth()) {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        }
        String database = mongodbConfig.getDatabase();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        Document connectionStatus = mongoDatabase.runCommand(new Document("connectionStatus", 1).append("showPrivileges", 1));
        Document isMaster = mongoDatabase.runCommand(new Document("isMaster", 1));

        if (isMaster.containsKey("msg") && "isdbgrid".equals(isMaster.getString("msg"))) {
            // validate mongos config.shards and source DB privileges
            return validateMongodb(connectionStatus);
        } else if (isMaster.containsKey("setName")) {   // validate replica set
//            if (!validateAuthDB(connectionStatus)) {
//                return false;
//            }
            if (!validateReadOrWriteDatabase(connectionStatus, database, READ_PRIVILEGE_ACTIONS)) {
                consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, "Missing read privileges on" + mongodbConfig.getDatabase() + "database"));
                return false;
            }
            if (!validateOplog(connectionStatus)) {
                return false;
            }

            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } else {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "Source mongodb instance must be the shards or replica set."));
            return false;
        }
    }

    @Override
    public Boolean testWritePrivilege() {
        if (!isOpenAuth()) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
            return true;
        }
        MongoDatabase mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
        Document connectionStatus = mongoDatabase.runCommand(new Document("connectionStatus", 1).append("showPrivileges", 1));
        if (!validateReadOrWriteDatabase(connectionStatus, mongodbConfig.getDatabase(), READ_WRITE_PRIVILEGE_ACTIONS)) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, "Missing readWrite privileges on" + mongodbConfig.getDatabase() + "database"));
            return false;
        }
        Document isMaster = mongoDatabase.runCommand(new Document("isMaster", 1));
        if (!isMaster.containsKey("msg") && !"isdbgrid".equals(isMaster.getString("msg")) && !isMaster.containsKey("setName")) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Warning: target is not replicaset or shards, can not use validator and progress feature."));
            return true;
        }
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }


    protected boolean validateReadOrWriteDatabase(Document connectionStatus, String database, Set<String> expectActions) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List authUserPrivileges = nodeAuthInfo.get("authenticatedUserPrivileges", List.class);

        if (CollectionUtils.isEmpty(authUserPrivileges)) {
            return false;
        }
        Map<String, Set<String>> resourcePrivilegesMap = adaptResourcePrivilegesMap(authUserPrivileges);
        if (!resourcePrivilegesMap.containsKey(database) && !resourcePrivilegesMap.containsKey("")) {
            return false;
        }
        Set<String> sourceDBPrivilegeSet = resourcePrivilegesMap.get(database);
        if (sourceDBPrivilegeSet == null) {
            sourceDBPrivilegeSet = resourcePrivilegesMap.get("");
        }
        if (sourceDBPrivilegeSet == null || !sourceDBPrivilegeSet.containsAll(expectActions)) {
            return false;
        }
        return true;
    }


    protected Map<String, Set<String>> adaptResourcePrivilegesMap(List<Document> authUserPrivileges) {
        Map<String, Set<String>> resourcePrivilegesMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(authUserPrivileges)) {
            for (Document authUserPrivilege : authUserPrivileges) {
                Document resource = authUserPrivilege.get("resource", Document.class);
                List actions = authUserPrivilege.get("actions", List.class);
                String db = resource.getString("db");
                String collection = resource.getString("collection");
                StringBuilder sb = new StringBuilder();
                if (EmptyKit.isNotBlank(db)) {
                    sb.append(db);
                    if (EmptyKit.isNotBlank(collection)) {
                        sb.append(".").append(collection);
                    }
                }
                if (!resourcePrivilegesMap.containsKey(sb.toString())) {
                    resourcePrivilegesMap.put(sb.toString(), new HashSet<>());
                }
                resourcePrivilegesMap.get(sb.toString()).addAll(actions);
            }
        }
        return resourcePrivilegesMap;
    }

    protected boolean isOpenAuth() {
        String databaseUri = mongodbConfig.getUri();
        String username = mongodbConfig.getUser();
        String password = mongodbConfig.getPassword();
        if (EmptyKit.isNotBlank(databaseUri)) {
            ConnectionString connectionString = new ConnectionString(databaseUri);
            username = connectionString.getUsername();
            char[] passwordChars = connectionString.getPassword();
            if (passwordChars != null && passwordChars.length > 0) {
                password = new String(passwordChars);
            }

        }
        if (EmptyKit.isBlank(username) || EmptyKit.isBlank(password)) {
            return false;
        }
        return true;
    }


    protected boolean validateMongodb(Document connectionStatus) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List authUserPrivileges = nodeAuthInfo.get("authenticatedUserPrivileges", List.class);

        Map<String, Set<String>> resourcePrivilegesMap = adaptResourcePrivilegesMap(authUserPrivileges);
        if (!resourcePrivilegesMap.containsKey(CONFIG_DATABASE) && !resourcePrivilegesMap.containsKey(CONFIG_DATABASE_SHARDS_COLLECTION)) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, "Missing mongos config.shards collection's read privileges." +
                    "will not be able to use the incremental sync feature."));
            return false;
        }
        Set<String> configDBPrivilegeSet = resourcePrivilegesMap.get(CONFIG_DATABASE);
        if (configDBPrivilegeSet == null) {
            configDBPrivilegeSet = resourcePrivilegesMap.get(CONFIG_DATABASE_SHARDS_COLLECTION);
        }

        if (configDBPrivilegeSet == null || !configDBPrivilegeSet.containsAll(READ_PRIVILEGE_ACTIONS)) {
            Set<String> missActions = new HashSet<>(READ_PRIVILEGE_ACTIONS);
            missActions.removeAll(configDBPrivilegeSet);
            StringBuilder sb = new StringBuilder();
            sb.append("Missing actions ").append(missActions).append(" on mongos config.shards collection, will not be able to use the incremental sync feature.");
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, String.valueOf(sb)));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }


    private boolean validateOplog(Document connectionStatus) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List authUserPrivileges = nodeAuthInfo.get("authenticatedUserPrivileges", List.class);
        if (CollectionUtils.isNotEmpty(authUserPrivileges)) {

            Map<String, Set<String>> resourcePrivilegesMap = adaptResourcePrivilegesMap(authUserPrivileges);
            if (!resourcePrivilegesMap.containsKey(LOCAL_DATABASE) && !resourcePrivilegesMap.containsKey(LOCAL_DATABASEOPLOG_COLLECTION)) {
                consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Missing local.oplog.rs collection's read privileges, will not be able to use the incremental sync feature."));
                return false;
            }
        }
        return true;
    }


    protected boolean validateAuthDB(Document connectionStatus) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List nodeAuthenticatedUsers = nodeAuthInfo.get("authenticatedUsers", List.class);
        if (CollectionUtils.isNotEmpty(nodeAuthenticatedUsers)) {
            Document nodeAuthUser = (Document) nodeAuthenticatedUsers.get(0);
            String nodeAuthDB = nodeAuthUser.getString("db");
            if (!ADMIN_DATABASE.equals(nodeAuthDB)) {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED,
                        "Authentication database is not admin, will not be able to use the incremental sync feature."));
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean testStreamRead() {
        Map<String, String> nodeConnURIs = MongodbUtil.nodesURI(mongoClient, mongodbConfig.getUri());
        if (nodeConnURIs.size() == 0 || nodeConnURIs.get("single") != null) {
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "mongodb standalone mode not support cdc."));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }


}

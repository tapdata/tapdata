package io.tapdata.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import io.tapdata.base.BaseTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Created by tapdata on 08/02/2018.
 */
@Ignore
public class MongodbTest extends BaseTest {

	public static final String ADMIN_DATABASE_NAME = "admin";
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();

	private List<MongodProcess> mongodProcessList = new ArrayList<>();
	private static MongoClient mongoClient;
	private ExecutorService service = Executors.newCachedThreadPool();

    /*@Before
    public void init() throws IOException, URISyntaxException {
        super.init();
        if (!isPortInUse("127.0.0.1", 54321)) {

            Map<String, List<IMongodConfig>>  replicaSets = new HashMap<>();
            List<IMongodConfig> replicasetList = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                replicasetList.add(new MongodConfigBuilder()
                        .version(Version.Main.PRODUCTION)
                        .net(new Net("127.0.0.1", 54321 + i, Network.localhostIsIPv6()))
                        .replication(new Storage(null, "unittest", 10))
                        .build());
            }
            replicaSets.put("unittest", replicasetList);

            try {

                for (Map.Entry<String, List<IMongodConfig>> entry : replicaSets.entrySet()) {
                    mongoClient = initializeReplicaSet(entry);
                }
                MongoDatabase database = mongoClient.getDatabase("demo");
                MongoCollection<Document> collection = database.getCollection("test");
                insertNorthwindData(collection);

                collection.renameCollection(new MongoNamespace("demo", "test123"));
                collection.createIndex(new Document("x", 1));
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                return;
            }
        }
    }

    @Test
    public void mongodbIntegration() throws Exception {
        String jobName = "mongo-connector";

        LinkedBlockingQueue<List<MessageEntity>> messageQueue = new LinkedBlockingQueue<>();
        Job job = getAssignJob(jobName);
        Connections targetConn = getAssignConnection(job.getConnections().getTarget());
        ClientMongoOperator mongoClient = getMongoClient(targetConn);
        EmbeddedEngine engine = (EmbeddedEngine) startUpJob(job, messageQueue);
        long count = 0;

        TransformerContext context = new TransformerContext(job, null, mongoClient, targetConn, messageQueue);
        Transformer transformer = new TransformerMongodb(context, new HashMap<>());
        Map<String, Long> stats = transformer.getStats();
        service.execute(transformer);

        while (true) {
            long processed = stats.getOrDefault("processed", 0L);
            if (processed >= 13) {
                Assert.assertEquals(13, processed);
                job.setStatus(ConnectorConstant.PAUSED);
                Thread.sleep(1000);
                break;
            } else if (count > 5) {
                Assert.fail();
                break;
            }
            count++;
            Thread.sleep(2000);
        }

        engine.stop();
        service.shutdown();
    }

    @Test
    public void mongodbProgressRate() throws Exception {
        String jobName = "mongo-connector";

        WarningMaker warningMaker = new WarningMaker(clientMongoOperator);
        JobProgressRateStats progressRateStats = new JobProgressRateStats(warningMaker);
        Job job = getAssignJob(jobName);
        Connections targetConn = getAssignConnection(job.getConnections().getTarget());
        Connections sourceConn = getAssignConnection(job.getConnections().getSource());
        MongoClient targetMongoClient = MongodbUtil.createMongoClient(targetConn);
        ProgressRateStats stats = progressRateStats.progressRateStats(job, sourceConn, targetConn, targetMongoClient, null);
        Assert.assertTrue(stats.getRow_count() != null);
        Assert.assertTrue(stats.getTs() != null);

        String mappingJson = "[{\"from_table\" : \"test123\",\"to_table\" : \"test123\"}, {\"from_table\" : \"test\",\"to_table\" : \"test\"}]";

        Map<String, String> offset = new HashMap<>();
        offset.put("{\"rs\":\"unittest\",\"server_id\":\"mongo-connector\"}", "{\"sec\":1530874151,\"ord\":1,\"h\":6371108894346800451}");
        List<Mapping> mappings = JSONUtil.json2List(mappingJson, Mapping.class);
        job.setMappings(mappings);
        job.setMapping_template(ConnectorConstant.MAPPING_TEMPLATE_CUSTOM);
        job.setOffset(offset);

        ProgressRateStats mappingStats = progressRateStats.progressRateStats(job, sourceConn, targetConn, targetMongoClient, null);
        Assert.assertTrue(mappingStats.getRow_count() != null);
        Assert.assertTrue(mappingStats.getTs() != null);
    }

    private ClientMongoOperator getMongoClient(Connections connections) throws UnsupportedEncodingException {
        MongoClient mongoClient = MongodbUtil.createMongoClient(connections);
        String databaseUri = connections.getDatabase_uri();
        String db = connections.getDatabase_name();
        if (StringUtils.isNotBlank(databaseUri)) {
            MongoClientURI uri = new MongoClientURI(databaseUri);
            db = uri.getDatabase();
        }
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, db);
        ClientMongoOperator clientMongoOperator = new ClientMongoOperator(mongoTemplate, mongoClient);
        return clientMongoOperator;
    }

    private void insertNorthwindData(MongoCollection<Document> collection) throws IOException, URISyntaxException {
        URL connURL = BaseTest.class.getClassLoader().getResource("dataset/northwind.json");

        List<Document> datas = new ArrayList<>();

        String northwindJson = new String(Files.readAllBytes(Paths.get(connURL.toURI())));
        List<Object> list = JSONUtil.json2List(northwindJson, Object.class);
        for (Object obj : list) {
            Map<String, Object> map = (Map<String, Object>) obj;
            Document document = new Document();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                document.put(entry.getKey(), entry.getValue());
            }
            datas.add(document);
        }

        collection.insertMany(datas);
    }

    private MongoClient initializeReplicaSet(Map.Entry<String, List<IMongodConfig>> entry)
            throws Exception {
        String replicaName = entry.getKey();
        List<IMongodConfig> mongoConfigList = entry.getValue();

        if (mongoConfigList.size() < 3) {
            throw new Exception(
                    "A replica set must contain at least 3 members.");
        }
        // Create 3 mongod processes
        for (IMongodConfig mongoConfig : mongoConfigList) {
            if (!mongoConfig.replication().getReplSetName().equals(replicaName)) {
                throw new Exception(
                        "Replica set name must match in mongo configuration");
            }
            MongodStarter starter = MongodStarter.getDefaultInstance();
            MongodExecutable mongodExe = starter.prepare(mongoConfig);
            MongodProcess process = mongodExe.start();
            mongodProcessList.add(process);
        }
        Thread.sleep(1000);
        MongoClientOptions mo = MongoClientOptions.builder()
                .connectTimeout(10)
                .build();
        MongoClient mongo = new MongoClient(new ServerAddress(mongoConfigList.get(0).net()
                .getServerAddress().getHostName(), mongoConfigList.get(0).net()
                .getPort()), mo);
        DB mongoAdminDB = mongo.getDB(ADMIN_DATABASE_NAME);

        CommandResult cr = mongoAdminDB.command(new BasicDBObject("isMaster", 1));

        // Build BSON object replica set settings
        DBObject replicaSetSetting = new BasicDBObject();
        replicaSetSetting.put("_id", replicaName);
        BasicDBList members = new BasicDBList();
        int i = 0;
        for (IMongodConfig mongoConfig : mongoConfigList) {
            DBObject host = new BasicDBObject();
            host.put("_id", i++);
            host.put("host", mongoConfig.net().getServerAddress().getHostName()
                    + ":" + mongoConfig.net().getPort());
            members.add(host);
        }

        replicaSetSetting.put("members", members);
        // Initialize replica set
        cr = mongoAdminDB.command(new BasicDBObject("replSetInitiate",
                replicaSetSetting));

        Thread.sleep(5000);
        cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));

        // Check replica set status before to proceed
        while (!isReplicaSetStarted(cr)) {
            Thread.sleep(1000);
            cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
        }

        return mongo;
    }

    private boolean isReplicaSetStarted(BasicDBObject setting) {
        if (setting.get("members") == null) {
            return false;
        }

        BasicDBList members = (BasicDBList) setting.get("members");
        for (Object m : members.toArray()) {
            BasicDBObject member = (BasicDBObject) m;
            int state = member.getInt("state");
            // 1 - PRIMARY, 2 - SECONDARY, 7 - ARBITER
            if (state != 1 && state != 2 && state != 7) {
                return false;
            }
        }
        return true;
    }

    @After
    public void stop() {
        super.stop();
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (!CollectionUtils.isEmpty(mongodProcessList)) {
            for (MongodProcess mongodProcess : mongodProcessList) {
                try {
                    mongodProcess.stop();
                } catch (Exception e) {
                }
            }
        }
    }

    @Test
    public void testCreateView() {

        String json = "[{\"$group\":{\"_id\":\"$商户账户编号\",\"totalAmount\":{\"$sum\":\"$交易金额\"}}},{\"$sort\":{\"totalAmount\":-1}}]";

        MongoClient client = new MongoClient(new MongoClientURI("mongodb://127.0.0.1:27017/target"));

        MongoDatabase db = client.getDatabase("target");

        BsonArray bson = BsonArray.parse(json);
        List<Bson> pipeline = new ArrayList<>(bson.size());
        bson.forEach(bsonValue -> {
            pipeline.add(bsonValue.asDocument());
        });
        String viewName = "MerchantStat1";
        //db.runCommand();
        Document filter = new Document();
        filter.append("name", viewName);
        ListCollectionsIterable<Document> collections = db.listCollections().filter(filter);
        for (Document col : collections) {
            System.out.println(col.toJson());
        }

        try {
            db.createView(viewName, "order", pipeline);
        } catch (MongoCommandException e) {
            System.out.println(e.getErrorCodeName());
            e.printStackTrace();
        }

    }*/

	@Test
	public void createTable() {
		MongoClientURI mongoClientURI = new MongoClientURI("mongodb://mongo:27017/source");
		try (
				MongoClient client = new MongoClient(mongoClientURI);
		) {
			MongoDatabase database = client.getDatabase(mongoClientURI.getDatabase());
			int tableCount = 500;
			IntStream.range(0, tableCount).forEach(i -> {
				MongoCollection<Document> collection = database.getCollection(RandomStringUtils.randomAlphanumeric(10) + i);
				collection.drop();
				collection.insertOne(new Document("id", RandomUtils.nextInt()).append("name", RandomStringUtils.randomAlphabetic(10)));
				System.out.println(i + "/" + tableCount);
			});
		}
	}
}

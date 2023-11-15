package io.tapdata.base;

import com.tapdata.mongo.ClientMongoOperator;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Created by tapdata on 23/01/2018.
 */
public class BaseTest {

	protected static final String mongoURI = "mongodb://localhost:12345/tapdata";

	protected static JdbcTemplate mysqlTemplate;

	/**
	 * please store Starter or RuntimeConfig in a final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();

	private static MongodExecutable mongodExe;
	private static MongodProcess mongod;

	private final static String BASE_URL = "http://localhost:8080/api/";

	private final static String access_token = "IwhxULXqQEAAqxF7LlH2O2hLLUHSZvn7VYBItftHvv1ymfNbMiqUn6DDZj8Nv2Mv";

	protected static ClientMongoOperator clientMongoOperator;
//
//    @Before
//    public void init() throws IOException, URISyntaxException {
//
//        if (!isPortInUse("127.0.0.1", 12345)) {
//            // initialize mongodb instance
//            mongodExe = starter.prepare(new MongodConfigBuilder()
//                    .version(Version.Main.PRODUCTION)
//                    .net(new Net("localhost", 12345, Network.localhostIsIPv6()))
//                    .build());
//            mongod = mongodExe.start();
//        }
//
//        // connect mongodb
////        MongoClientURI mongoClientURI = new MongoClientURI(mongoURI);
////        MongoClient mongoClient = new MongoClient(mongoClientURI);
////        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, mongoClientURI.getDatabase());
//
//        ConfigurationCenter configCenter = new ConfigurationCenter();
//        configCenter.putConfig(ConfigurationCenter.TOKEN, access_token);
//
//        RestTemplateOperator restTemplateOperator = new RestTemplateOperator(BASE_URL);
//        clientMongoOperator = new HttpClientMongoOperator(null, null, restTemplateOperator, configCenter);
//
//        URL connURL = BaseTest.class.getClassLoader().getResource("dataset/Connections.json");
//        URL jobsURL = BaseTest.class.getClassLoader().getResource("dataset/Jobs.json");
//
//        String connectionsJson = new String(Files.readAllBytes(Paths.get(connURL.toURI())));
//        String jobsJson = new String(Files.readAllBytes(Paths.get(jobsURL.toURI())));
//
//        List<Connections> list = JSONUtil.json2List(connectionsJson, Connections.class);
//        List<Job> jobs = JSONUtil.json2List(jobsJson, Job.class);
//
//        for (Job job : jobs) {
//            clientMongoOperator.delete(new Query(where("name").is(job.getName())), ConnectorConstant.JOB_COLLECTION);
//        }
//
//        for (Connections connections : list) {
//            clientMongoOperator.delete(new Query(where("_id").is(connections.getId())), ConnectorConstant.CONNECTION_COLLECTION);
//
//        }
////        clientMongoOperator.dropCollection(ConnectorConstant.JOB_COLLECTION);
////        clientMongoOperator.dropCollection(ConnectorConstant.CONNECTION_COLLECTION);
//
//        clientMongoOperator.insertList(jobs, ConnectorConstant.JOB_COLLECTION);
//        clientMongoOperator.insertList(list, ConnectorConstant.CONNECTION_COLLECTION);
//    }
//
//    protected static void initJDBCTemplate() {
//        if (mysqlTemplate == null) {
//            DriverManagerDataSource dataSource = new DriverManagerDataSource();
//            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
//            dataSource.setUrl("jdbc:mysql://localhost:2215?autoReconnect=true&useSSL=false");
//            dataSource.setUsername("unittest");
//            dataSource.setPassword("unittest");
//
//            mysqlTemplate = new JdbcTemplate(dataSource);
//        }
//    }
//
//    protected JdbcTemplate getJdbcTemplate(Connections connections) {
//        if (connections != null) {
//            String databaseType = connections.getDatabase_type();
//            String host = connections.getDatabase_host();
//            Integer port = connections.getDatabase_port();
//            String databaseName = connections.getDatabase_name();
//            String username = connections.getDatabase_username();
//            String password = connections.getDatabase_password();
//            DriverManagerDataSource dataSource = new DriverManagerDataSource();
//            if (DatabaseTypeEnum.ORACLE.getType().equals(databaseType)) {
//                dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
//                StringBuilder sb = new StringBuilder("jdbc:oracle:thin:@").append(host).append(":").append(port).append(":").append(databaseName);
//                dataSource.setUrl(sb.toString());
//            } else {
//                StringBuilder sb = new StringBuilder("jdbc:mysql://").append(host).append(":").append(port).append("?autoReconnect=true&useSSL=false");
//                dataSource.setDriverClassName("com.mysql.jdbc.Driver");
//                dataSource.setUrl(sb.toString());
//            }
//            dataSource.setUsername(username);
//            dataSource.setPassword(password);
//
//            return new JdbcTemplate(dataSource);
//        }
//        return null;
//    }
//
//    protected Job getAssignJob(String jobName) {
//        List<Job> jobs = clientMongoOperator.find(new Query(where("name").is(jobName)), ConnectorConstant.JOB_COLLECTION, Job.class);
//        return jobs.get(0);
//    }
//
//    protected Connections getAssignConnection(String connectionId) {
//        List<Connections> connections = clientMongoOperator.find(new Query(where("_id").is(connectionId)), ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
//        return connections.get(0);
//    }
//
//    protected void loadJsonFile(String collection, String filePath) throws URISyntaxException, IOException {
//        URL connURL = BaseTest.class.getClassLoader().getResource(filePath);
//        String json = new String(Files.readAllBytes(Paths.get(connURL.toURI())));
//        List<Document> list = JSONUtil.json2List(json, Document.class);
//        List<Document> documents = new ArrayList<>(list.size());
//        for (Document document : list) {
//            documents.add(Document.parse(document.toJson()));
//        }
//        clientMongoOperator.insertList(documents, collection);
//    }
//
//    protected Connector startUpJob(Job job, LinkedBlockingQueue<List<MessageEntity>> messageQueue) throws Exception {
//        JobConnection connections = job.getConnections();
//        String source = connections.getSource();
//        Connections connection = getAssignConnection(source);
//
//        JobManager jobManager = new JobManager();
//        Connector connector = jobManager.prepare(mongoURI, job, clientMongoOperator, connection, messageQueue, null, null, null, null, null, 3, null, null, 1);
//        connector.start();
//        return connector;
//    }
//
//    @After
//    public void stop() {
//
//        if (clientMongoOperator != null) {
//            clientMongoOperator.releaseResource();
//        }
//
//        if (mongod != null) {
//            mongod.stop();
//        }
//        if (mongodExe != null) {
//            mongodExe.stop();
//        }
//
//
//    }
//
//    protected static boolean isPortInUse(String hostName, int portNumber) {
//        boolean result;
//
//        try {
//
//            Socket s = new Socket(hostName, portNumber);
//            s.close();
//            result = true;
//
//        } catch (Exception e) {
//            result = false;
//        }
//
//        return (result);
//    }

}

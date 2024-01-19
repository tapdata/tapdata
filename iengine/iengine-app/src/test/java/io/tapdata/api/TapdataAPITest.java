package io.tapdata.api;

import com.tapdata.mongo.ClientMongoOperator;

public class TapdataAPITest {

	private final static String BASE_URL = "http://localhost:8080/api/";

	private final static String access_token = "zT1nvhKqSxFVABtvIx5dAHijDORC6RMZ3w65NLmvgSBg7U7NIwvfvuj1CN37cqWH";

	private ClientMongoOperator clientMongoOperator;

//    @Before
//    public void init() {
//
//        ConfigurationCenter configCenter = new ConfigurationCenter();
//        configCenter.putConfig(ConfigurationCenter.TOKEN, access_token);
//
//        RestTemplateOperator restTemplateOperator = new RestTemplateOperator(BASE_URL);
//        clientMongoOperator = new HttpClientMongoOperator(null, null, restTemplateOperator, configCenter);
//    }
//
//    @Test
//    public void insertLogs(){
//
//        String bsonStr = "{ \"level\" : \"INFO\", \"loggerName\" : \"com.tapdata.Application\", \"message\" : \"12312344\", \"source\" : { \"className\" : \"org.springframework.boot.StartupInfoLogger\", \"methodName\" : \"logStarted\", \"fileName\" : \"StartupInfoLogger.java\", \"lineNumber\" : 59 }, \"marker\" : null, \"threadId\" : NumberLong(1), \"threadName\" : \"main\", \"threadPriority\" : 5, \"millis\" : NumberLong(\"1540008112723\"), \"date\" : ISODate(\"2018-10-20T04:01:52.723Z\"), \"thrown\" : null, \"contextMap\" : {  }, \"contextStack\" : [ ] }";
//
//        Document log = Document.parse(bsonStr);
//        ObjectId _id = new ObjectId();
//        log.put("id", _id.toHexString());
//
//        System.out.println(_id.toHexString());
////        Map<String, Object> token = addToken();
//        clientMongoOperator.insertOne(log, ConnectorConstant.LOG_COLLECTION);
////        restTemplateOperator.postOne(log, ConnectorConstant.LOG_COLLECTION, token);
//    }
//
//    @Test
//    public void getConnections() throws IllegalAccessException {
//
//        List<Connections> batch = clientMongoOperator.find(new HashMap<>(), ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
//
//        Assert.assertTrue(CollectionUtils.isNotEmpty(batch));
//
//        Connections connections = batch.get(0);
//        String id = connections.getId();
//
//        Map<String, Object> params = new HashMap<>();
//        params.put("id", id);
//        params.putAll(addToken());
//
//        List<Connections> batch1 = clientMongoOperator.find(params, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
//
//        Assert.assertTrue(CollectionUtils.isNotEmpty(batch1));
//    }
//
//    @Test
//    public void findAndModify() throws IllegalAccessException {
//
//        UUID uuid = UUID.randomUUID();
//        String jobName = uuid.toString();
//
//        String jobStr = "{'name' : '"+ jobName +"', 'priority' : 'normal', 'status' : 'scheduled', 'mappings' : null, 'deployment' : { 'sync_point' : 'beginning', 'sync_time' : '' }, 'sync_type' : 'initial_sync+cdc', 'mapping_template' : 'cluster-clone', 'notification_interval' : 300, 'notification_window' : 0, 'is_validate' : false, 'custom_sql' : '', 'multi_thread' : false, 'source_read' : 20000, 'initial_offset' : '', 'op_filters' : null, 'event_job_started' : false, 'event_job_editted' : false, 'event_job_error' : false, 'event_job_stopped' : false, 'drop_target' : false, 'is_null_write' : false, 'is_test_write' : false, 'test_write' : { 'rows' : 100, 'col_length' : 50, 'is_bulk_result' : false }, 'increment' : false, 'incrementInterval' : 1, 'connectorStopped' : true, 'transformerStopped' : true, 'last_update' : NumberLong('1540543140502'), 'ping_time' : null, 'storeInstanceNo' : null, 'connector_ping_time' : null, 'first_ts' : null, 'fullSyncSucc' : false, 'lag' : null, 'last_ts' : null, 'validate_offset' : null, 'row_count' : { 'lag' : NumberLong(0), 'source' : NumberLong(35), 'lag_percentage' : NumberLong(0), 'target' : NumberLong(35) }, 'ts' : { 'lag' : NumberLong(0), 'source' : '2018-10-22 19:57:53', 'lag_percentage' : null, 'target' : '2018-10-22 19:57:53' }, 'offset' : { 'tablesOffset' : { 'orderlines' : NumberLong(5), 'orders' : NumberLong(1), 'details' : NumberLong(4), 'authors' : NumberLong(0), 'customer' : NumberLong(2) }, 'syncStage' : 'cdc', '_class' : 'com.tapdata.entity.SybaseCDCOffset' }, 'stats':{}, 'connections':{} }";
//
//        Document jobDoc = Document.parse(jobStr);
//
//        clientMongoOperator.insertOne(jobDoc, ConnectorConstant.JOB_COLLECTION);
//
//        Query query = runningJobQuery();
//
//        Update update = runningJobUpdate();
//
//        Job modifiedJob = clientMongoOperator.findAndModify(query, update, Job.class, ConnectorConstant.JOB_COLLECTION, true);
//
//        Assert.assertTrue(jobName.equals(modifiedJob.getName()));
//
//
//        clientMongoOperator.delete(new Query(where("name").is(jobName)), ConnectorConstant.JOB_COLLECTION);
//    }
//
//    @Test
//    public void update() throws IllegalAccessException {
//
//        UUID uuid = UUID.randomUUID();
//        String jobName = uuid.toString();
//
//        String jobStr = "{'name' : '"+ jobName +"', 'priority' : 'normal', 'status' : 'scheduled', 'mappings' : null, 'deployment' : { 'sync_point' : 'beginning', 'sync_time' : '' }, 'sync_type' : 'initial_sync+cdc', 'mapping_template' : 'cluster-clone', 'notification_interval' : 300, 'notification_window' : 0, 'is_validate' : false, 'custom_sql' : '', 'multi_thread' : false, 'source_read' : 20000, 'initial_offset' : '', 'op_filters' : null, 'event_job_started' : false, 'event_job_editted' : false, 'event_job_error' : false, 'event_job_stopped' : false, 'drop_target' : false, 'is_null_write' : false, 'is_test_write' : false, 'test_write' : { 'rows' : 100, 'col_length' : 50, 'is_bulk_result' : false }, 'increment' : false, 'incrementInterval' : 1, 'connectorStopped' : true, 'transformerStopped' : true, 'last_update' : NumberLong('1540543140502'), 'ping_time' : null, 'storeInstanceNo' : null, 'connector_ping_time' : null, 'first_ts' : null, 'fullSyncSucc' : false, 'lag' : null, 'last_ts' : null, 'validate_offset' : null, 'row_count' : { 'lag' : NumberLong(0), 'source' : NumberLong(35), 'lag_percentage' : NumberLong(0), 'target' : NumberLong(35) }, 'ts' : { 'lag' : NumberLong(0), 'source' : '2018-10-22 19:57:53', 'lag_percentage' : null, 'target' : '2018-10-22 19:57:53' }, 'offset' : { 'tablesOffset' : { 'orderlines' : NumberLong(5), 'orders' : NumberLong(1), 'details' : NumberLong(4), 'authors' : NumberLong(0), 'customer' : NumberLong(2) }, 'syncStage' : 'cdc', '_class' : 'com.tapdata.entity.SybaseCDCOffset' }, 'stats':{'validate_stats' : { 'total' : 0, 'error' : 0, 'success' : 0 }, 'total' : { 'source_received' : 0, 'processed' : 0, 'target_inserted' : 0, 'target_updated' : 0 }}, 'connections':{} }";
//
//        Document jobDoc = Document.parse(jobStr);
//
//        clientMongoOperator.insertOne(jobDoc, ConnectorConstant.JOB_COLLECTION);
//
//        Query query = new Query(where("name").is(jobName));
//
//        Update update = new Update().set("stats.validate_stats.total", 1024);
//
//        UpdateResult updateResult = clientMongoOperator.update(query, update, ConnectorConstant.JOB_COLLECTION);
//
//        Assert.assertTrue(updateResult.getModifiedCount() == 1);
//
//        List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
//
//        Assert.assertTrue(CollectionUtils.isNotEmpty(jobs));
//
//        Job job = jobs.get(0);
//        Long total = job.getStats().getValidate_stats().get("total");
//
//        Assert.assertNotNull(total);
//        Assert.assertTrue(total == 1024);
////        Assert.assertTrue(jobName.equals(modifiedJob.getName()));
//
//
//        clientMongoOperator.delete(new Query(where("name").is(jobName)), ConnectorConstant.JOB_COLLECTION);
//    }
//
//    private Map<String, Object> addToken(){
//        Map<String, Object> tokenMap = new HashMap<>();
//        tokenMap.put("access_token", access_token);
//        return tokenMap;
//    }
//
//    private Query runningJobQuery() {
//        Query mongoConnQuery = new Query(where("database_type").is("mongoddb"));
//        List<Connections> connections = clientMongoOperator.find(mongoConnQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
//        List<String> userIds = new ArrayList<>();
//        List<String> sourceNames = new ArrayList<>();
//        connections.forEach(connection -> {
//            userIds.add(connection.getUser_id());
//            sourceNames.add(connection.getName());
//        });
//
//        Criteria scheduledCriteria = where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.SCHEDULED);
//        Criteria runningCriteria = where(ConnectorConstant.JOB_STATUS_FIELD).is(ConnectorConstant.RUNNING);
////        Criteria mongoConnCriteria = new Criteria().orOperator(where("connections.source").nin(sourceNames), where("connections.source").in(sourceNames).and("user_id").nin(userIds));
//
//        Calendar calendar = Calendar.getInstance();
//        calendar.add(Calendar.MINUTE, -1);
//        Criteria pingTimeCriteria = new Criteria().orOperator(
//                where("connector_ping_time").lt(calendar.getTimeInMillis()),
//                where("connector_ping_time").exists(false),
//                where("connector_ping_time").is(null)
//        );
//
//        Query query = new Query(
//                new Criteria().orOperator(scheduledCriteria,
//                        new Criteria().andOperator(runningCriteria, pingTimeCriteria)
//                )
//        );
//
//        return query;
//    }
//
//    private Update runningJobUpdate(){
//        Update update = new Update().set(ConnectorConstant.JOB_STATUS_FIELD, ConnectorConstant.RUNNING)
//                .set("storeInstanceNo", "unit-test-instance")
//                .set("connector_ping_time", System.currentTimeMillis());
//
//
//        return update;
//    }
}

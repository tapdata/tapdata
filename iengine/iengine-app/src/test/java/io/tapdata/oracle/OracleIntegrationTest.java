package io.tapdata.oracle;

import io.tapdata.base.BaseTest;

/**
 * Created by tapdata on 15/12/2017.
 */
public class OracleIntegrationTest extends BaseTest {

//    @Test
//    public void integratrionOracleTest() throws Exception {
//
//        String testJobName = "oracle-sync-time";
//        Job job = getAssignJob(testJobName);
//        JobConnection connections = job.getConnections();
//        ObjectId source = connections.getSource();
//        Connections oracleConnections = getAssignConnection(source);
//        LinkedBlockingQueue messageQueue = new LinkedBlockingQueue();
//        WarningMaker maker = new WarningMaker(new ArrayList<>());
//        Runnable runnable = JobManager.prepare(mongoURI, job, clientMongoOperator, oracleConnections, messageQueue, maker);
//
//        Thread t = new Thread(runnable);
//        t.start();
//        int count = 0;
//        while (true) {
////            if (job.getOffset() != null) {
////
////                job.setStatus(ConnectorConstant.PAUSED);
////                break;
////            } else if (count >= 5) {
////                Assert.fail();
////                break;
////            }
////
////            if (count <= 5) {
////                count++;
////                Thread.sleep(2000);
////                continue;
////            }
//        }
//    }

//    @Test
//    public void oracleProgressRateTest () throws IOException {
//        String testJobName = "northwind";
//        Job job = getAssignJob(testJobName);
//        JobConnection connections = job.getConnections();
//        String source = connections.getSource();
//        String target = connections.getTarget();
//
//        Connections sourceConn = getAssignConnection(source);
//        Connections targetConn = getAssignConnection(target);
//        MongoClient targetMongoClient = MongodbUtil.createMongoClient(targetConn);
//
//        DatabaseProgressRate databaseProgressRate = new OracleProgressRate();
//        ProgressRateStats progressRateStats = databaseProgressRate.progressRateInfo(job, sourceConn, targetConn, targetMongoClient, null);
//        Assert.assertTrue(progressRateStats != null);
//        Map<String, Object> rowCount = progressRateStats.getRow_count();
//        Assert.assertTrue(MapUtils.isNotEmpty(rowCount));
//
//        String mappingsJson = "[{\"from_table\" : \"CUSTOMERS\",\"custom_sql\" : \"\",\"offset\" : \"\",\"to_table\" : \"customers\",\"join_condition\" : [{" +
//                "\"source\" : \"CUSTOMER_ID\",\"target\" : \"CUSTOMER_ID\"}],\"relationship\" : \"OneOne\",\"target_path\" : \"\"},{\"from_table\" : \"ORDERS\",\"custom_sql\" : \"\",\"offset\" : \"\",\"to_table\" : \"customers\",\"join_condition\" : [{\"source\" : \"CUSTOMER_ID\",\"target\" : \"CUSTOMER_ID\"}]," +
//                "\"relationship\" : \"ManyOne\",\"target_path\" : \"ORDERS\",\"match_condition\" : [{\"source\" : \"ORDER_ID\",\"target\" : \"ORDERS.ORDER_ID\"}]}]";
//        job.setMapping_template(ConnectorConstant.MAPPING_TEMPLATE_CUSTOM);
//        job.setMappings(JSONUtil.json2List(mappingsJson, Mapping.class));
//
//        progressRateStats = databaseProgressRate.progressRateInfo(job, sourceConn, targetConn, targetMongoClient, null);
//        Assert.assertTrue(progressRateStats != null);
//        rowCount = progressRateStats.getRow_count();
//        Assert.assertTrue(MapUtils.isNotEmpty(rowCount));
//
//        Map<String, Object> deployment = new HashMap<>();
//        deployment.put(ConnectorConstant.SYNC_POINT_FIELD, ConnectorConstant.SYNC_POINT_SYNC_TIME);
//        deployment.put(ConnectorConstant.SYNC_TIME_FIELD, "2018-01-01 00:00");
//        job.setDeployment(deployment);
//        job.setSync_type(ConnectorConstant.SYNC_TYPE_CDC);
//
//        progressRateStats = databaseProgressRate.progressRateInfo(job, sourceConn, targetConn, targetMongoClient, null);
//        Assert.assertTrue(progressRateStats != null);
//        Map<String, Object> ts = progressRateStats.getTs();
//        Assert.assertTrue(MapUtils.isNotEmpty(ts));
//    }

//    @Test
//    public void syncTimeTest() throws Exception {
//        String testJobName = "oracle-sync-time";
//        Job job = getAssignJob(testJobName);
//        JobConnection connections = job.getConnections();
//        String source = connections.getSource();
//        Connections oracleConnections = getAssignConnection(source);
//        LinkedBlockingQueue messageQueue = new LinkedBlockingQueue();
//        Runnable runnable = JobManager.prepare(mongoURI, job, clientMongoOperator, oracleConnections, messageQueue, targetConn);
//
//        Thread t = new Thread(runnable);
//        t.start();
//
//        String messageCollection = new StringBuilder().append(ConnectorConstant.MESSAGE_COLLECTION).append("_").append(job.getId()).toString();
//
//        Map<String, Object> params = new HashMap<>();
//        params.put("condition", null);
//
//        int count = 0;
//        while (true) {
//            List<MessageEntity> messageEntities = clientMongoOperator.find(params, messageCollection, MessageEntity.class);
//
//            if (CollectionUtils.isEmpty(messageEntities) && count <= 5) {
//                count++;
//                Thread.sleep(2000);
//                continue;
//            }
//            job.setStatus(ConnectorConstant.PAUSED);
//            Assert.assertTrue(!CollectionUtils.isEmpty(messageEntities));
//            break;
//        }
//    }

//    @Test
//    public void pausedAndSstart() throws Exception{
//        String testJobName = "northwind";
//        Job job = getAssignJob(testJobName);
//        JobConnection connections = job.getConnections();
//        String source = connections.getSource();
//        Connections oracleConnections = getAssignConnection(source);
//
//        startUpJob(job);
//
//        while (true) {
//            if (job.getOffset() != null) {
//                job.setStatus(ConnectorConstant.PAUSED);
//
//                Long sourceReceived = new Long(job.getStats().getTotal().get("source_received"));
//                JdbcTemplate jdbcTemplate = getJdbcTemplate(oracleConnections);
//                String insertSql = "insert into categories values(9, 'tapdata-test','tapdata-test','tapdata-test')";
//                String deleteSql = "delete from categories where CATEGORY_ID = 9";
//                jdbcTemplate.execute(insertSql);
//                jdbcTemplate.execute(deleteSql);
//
//                job.setStatus(ConnectorConstant.RUNNING);
//                startUpJob(job);
//
//                for (int i = 0; i < 5; i++) {
//                    Long sourceReceived1 = job.getStats().getTotal().get("source_received");
//                    if (sourceReceived1 > sourceReceived) {
//                        Assert.assertTrue(true);
//                        break;
//                    } else if (i >= 4) {
//                        Assert.fail();
//                    }
//                    Thread.sleep(5000);
//                }
//                job.setStatus(ConnectorConstant.PAUSED);
//                break;
//            }
//            Thread.sleep(1000);
//        }
//
//    }

}

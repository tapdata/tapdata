package io.tapdata.oracle;

import io.tapdata.base.BaseTest;

/**
 * Created by tapdata on 19/03/2018.
 */
public class OracleValidateTest extends BaseTest {

//    @Test
//    public void oracleValidateTest () throws Exception {
//
//        String testJobName = "northwind";
//        Job job = getAssignJob(testJobName);
//        loadJsonFile("validate_" + job.getId(), "dataset/oracle-northwind-validate.json");
//        job.setIs_validate(true);
//        JobConnection connections = job.getConnections();
//        String source = connections.getSource();
//        String target = connections.getTarget();
//        Connections oracleConnections = getAssignConnection(source);
//        Connections targetConn = getAssignConnection(target);
//
//        JobManager.generateClusterCloneMappings(job, oracleConnections);
//
//        Connection connection = OracleUtil.createConnection(oracleConnections);
//        OracleNumberConvert oracleValueConvert = new OracleNumberConvert(oracleConnections.getSchema(), job.getMappings());
//
//        JdbcSql oracleSql = JdbcSql.initSql(job, oracleConnections);
//
//        StringBuilder sb = new StringBuilder("message_").append(job.getId());
//        OracleConnectorContext context = new OracleConnectorContext(job, oracleConnections, 0, null, clientMongoOperator, connection, oracleSql, sb.toString());
//        context.setOracleValueConvert(oracleValueConvert);
//
//        OracleDataValidator validator = new OracleDataValidator(job, clientMongoOperator, context.getValidateCollection(), oracleValueConvert, connection);
//        new Thread(validator).start();
//
//        for (int i = 0; i<= 5; i++) {
//            Stats progressRateStats = job.getStats();
//            if (progressRateStats == null) {
//                Thread.sleep(1000);
//                continue;
//            }
//            Map<String, Long> validateStats = progressRateStats.getValidate_stats();
//            if (validateStats == null) {
//                Thread.sleep(1000);
//                continue;
//            }
//            Long total = validateStats.getOrDefault("total", new Long(0));
//            if (total.equals(14L)) {
//                Long pass = validateStats.getOrDefault("success", new Long(0));
//                Long error = validateStats.getOrDefault("error", new Long(0));
//                Assert.assertEquals(14L, total.longValue());
//                Assert.assertEquals(14L, pass.longValue());
//                Assert.assertEquals(0L, error.longValue());
//                job.setIs_validate(false);
//
//                Thread.sleep(1000);
//                break;
//            } else if (i == 5) {
//                Assert.fail();
//            }
//            Thread.sleep(5000);
//        }
//    }

}

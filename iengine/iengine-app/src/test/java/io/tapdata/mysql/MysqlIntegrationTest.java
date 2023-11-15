package io.tapdata.mysql;

import com.wix.mysql.EmbeddedMysql;
import io.tapdata.base.BaseTest;

/**
 * Created by tapdata on 16/12/2017.
 */
public class MysqlIntegrationTest extends BaseTest {

	private static EmbeddedMysql mysqld;
//
//    @Before
//    public void init() throws IOException, URISyntaxException {
//        super.init();
//        if (!isPortInUse("127.0.0.1", 2215)) {
//            System.out.println("Start mysql instance for unit test");
//            MysqldConfig config = aMysqldConfig(v5_7_latest)
//                    .withCharset(UTF8)
//                    .withPort(2215)
//                    .withUser("unittest", "unittest")
//                    .withTimeZone("Europe/Vilnius")
//                    .withTimeout(2, TimeUnit.MINUTES)
//                    .withServerVariable("max_connect_errors", 666)
//                    .withServerVariable("server-id", 223344)
//                    .withServerVariable("log_bin", "mysql-bin")
//                    .withServerVariable("expire_logs_days", 1)
//                    .withServerVariable("binlog_format", "row")
//                    .build();
//            mysqld = EmbeddedMysql.anEmbeddedMysql(config)
//                    .addSchema("inventory", ScriptResolver.classPathScript("dataset/mysql-inventory.sql"))
//                    .start();
//
//        }
//        initJDBCTemplate();
//    }
//
//    @Test
//    public void integrationTest() throws Exception {
//
//        String jobName = "inventory-integration";
//
//        Job job = getAssignJob(jobName);
//        EmbeddedEngine embeddedEngine = null;
//        try {
//            Connector connector = startUpJob(job, new LinkedBlockingQueue<>());
//            for (int i = 0; i < 5; i++) {
//                if (job.getOffset() != null) {
//                    break;
//                } else if (i == 4) {
//                    connector.stop(ConnectorConstant.FORCE_STOPPING);
//                    Assert.fail("Mysql integration unit test fail.");
//                    break;
//                }
//                Thread.sleep(1000);
//            }
//            job.setStatus(ConnectorConstant.PAUSED);
//        } finally {
//            if (embeddedEngine != null) {
//                // stop the job
//                embeddedEngine.stop();
//                Thread.sleep(1000);
//            }
//        }
//
//    }
//
//    @Test
//    public void pausedAndStartTest() throws Exception {
//        String jobName = "inventory-integration";
//        Job job = getAssignJob(jobName);
//        EmbeddedEngine embeddedEngine = null;
//        try {
//            Runnable engine = startUpJob(job);
//            embeddedEngine = (EmbeddedEngine) engine;
//            while (true) {
//                long count = getJobMessageCount(job);
//                if (count == 0) {
//                    continue;
//                }
//                embeddedEngine.stop();
//
//                updateOffset(job);
//                Thread t = new Thread(engine);
//                t.start();
//
//                for (int i = 0; i < 5; i++) {
//                    count = getJobMessageCount(job);
//                    if (count >= 26) {
//                        Assert.assertTrue(count >= 26);
//                        break;
//                    } else if (i == 4) {
//                        Assert.fail("Mysql syncPointTest unit test fail.");
//                        break;
//                    }
//                    Thread.sleep(2000);
//                }
//                job.setStatus(ConnectorConstant.PAUSED);
//                break;
//            }
//        } finally {
//            if (embeddedEngine != null) {
//                // stop the job
//                embeddedEngine.stop();
//            }
//        }
//
//    }
//
//    @Test
//    public void syncPointTest() throws Exception {
//        String jobName = "inventory-sync-point";
//
//        Job job = getAssignJob(jobName);
//        EmbeddedEngine embeddedEngine = null;
//        try {
//            // wait 1 second for sync new mysql record
//            Thread.sleep(1000);
//            Runnable engine = startUpJob(job);
//            embeddedEngine = (EmbeddedEngine) engine;
//            String isnertSql = "INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('tapdata', 'huang', 'freetapdatalife@gmail.com')";
//            String updateSql = "UPDATE inventory.customers SET last_name = 'hung' WHERE first_name = 'tapdata' AND last_name = 'huang' AND email = 'freetapdatalife@gmail.com'";
//            String deleteSql = "DELETE FROM inventory.customers WHERE first_name = 'tapdata' AND last_name = 'hung' AND email = 'freetapdatalife@gmail.com'";
//
//            mysqlTemplate.execute(isnertSql);
//            mysqlTemplate.execute(updateSql);
//            mysqlTemplate.execute(deleteSql);
//            for (int i = 0; i < 5; i++) {
//                long count = getJobMessageCount(job);
//                if (count == 3) {
//                    Assert.assertEquals(3, count);
//                    break;
//                } else if (i == 4) {
//                    Assert.fail("Mysql syncPointTest unit test fail.");
//                    break;
//                }
//                Thread.sleep(2000);
//            }
//        } finally {
//            if (embeddedEngine != null) {
//                // stop the job
//                embeddedEngine.stop();
//            }
//        }
//
//
//    }
//
//    @After
//    public void stop(){
//        super.stop();
//        if (mysqld != null) {
//            System.out.println("Stop mysql instance from unit test");
//            mysqld.stop();
//        }
//    }
}

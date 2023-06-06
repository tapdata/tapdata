import io.tapdata.connector.postgres.cdc.PostgresCdcRunner;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;

import java.util.Collections;

public class Main2 {
    public static void main(String[] args) throws Throwable {
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost("192.168.1.189");
        postgresConfig.setPort(5432);
        postgresConfig.setDatabase("COOLGJ");
        postgresConfig.setSchema("public");
        postgresConfig.setExtParams("");
        postgresConfig.setUser("postgres");
        postgresConfig.setPassword("gj0628");
        PostgresOffset postgresOffset = new PostgresOffset();
        postgresOffset.setSourceOffset("{\"lsn_proc\":186212648,\"lsn\":186212648,\"txId\":7526,\"ts_usec\":1653308485609993}");
//        PostgresCdcRunner cdcRunner = new PostgresCdcRunner(postgresConfig)
//                .useConfig(postgresConfig)
//                .watch(Collections.singletonList("Student"))
//                .offset(null)
//                .registerConsumer(null, 10);
//        new Thread(cdcRunner::startCdcRunner).start();
//        Thread.sleep(150000);
//
//        cdcRunner.closeCdcRunner();
    }
}

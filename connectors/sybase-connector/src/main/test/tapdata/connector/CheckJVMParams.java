package tapdata.connector;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.sybase.extend.ConnectionConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckJVMParams {
    @Test
    public void checkJVMParams() {
        ConnectionConfig connectionConfig = new ConnectionConfig(new DataMap());
        String jvm = "-XX:+UseG1GC\n" +
                "-XX:InitialHeapSize=2g\n" +
                "-XX:MaxHeapSize=95g\n" +
                "-XX:MaxGCPauseMillis=2000\n" +
                "-XX:+DisableExplicitGC\n" +
                "-XX:+UseStringDeduplication\n" +
                "-XX:+ParallelRefProcEnabled\n" +
                "-XX:MaxMetaspaceSize=1g\n" +
                "-XX:MaxTenuringThreshold=1\n" +
                "-XX:-UseCompressedOops\n" +
                "-XX:+PrintGCDetails";
        String jvmTemp = connectionConfig.withToolJavaOptionsLine(jvm);
        String expectedRes = "\"-XX:+UseG1GC\" \"-XX:InitialHeapSize=2g\" \"-XX:MaxHeapSize=95g\" \"-XX:MaxGCPauseMillis=2000\" \"-XX:+DisableExplicitGC\" \"-XX:+UseStringDeduplication\" \"-XX:+ParallelRefProcEnabled\" \"-XX:MaxMetaspaceSize=1g\" \"-XX:MaxTenuringThreshold=1\" \"-XX:-UseCompressedOops\" \"-XX:+PrintGCDetails\"";
        Assertions.assertEquals(
                expectedRes,
                jvmTemp,
                "Can not handel jvm param, jvm param: "
                        + jvm + ", handel result: "
                        + jvmTemp +
                        ", but  Expected results is: " + expectedRes);

        jvm = "-XX:+UseG1GC\r\n" +
                "-XX:InitialHeapSize=2g\r\n" +
                "-XX:MaxHeapSize=95g\r\n" +
                "-XX:MaxGCPauseMillis=2000\r\n" +
                "-XX:+DisableExplicitGC\r\n" +
                "-XX:+UseStringDeduplication\r\n" +
                "-XX:+ParallelRefProcEnabled\r\n" +
                "-XX:MaxMetaspaceSize=1g\r\n" +
                "-XX:MaxTenuringThreshold=1\r\n" +
                "-XX:-UseCompressedOops\r\n" +
                "-XX:+PrintGCDetails";
        jvmTemp = connectionConfig.withToolJavaOptionsLine(jvm);
        Assertions.assertEquals(
                expectedRes,
                jvmTemp,
                "Can not handel jvm param, jvm param: "
                        + jvm + ", handel result: "
                        + jvmTemp +
                        ", but  Expected results is: " + expectedRes);



    }
}

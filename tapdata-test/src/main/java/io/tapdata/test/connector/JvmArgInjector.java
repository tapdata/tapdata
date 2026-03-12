package io.tapdata.test.connector;

import java.lang.instrument.Instrumentation;

public class JvmArgInjector {
    public static void premain(String agentArgs, Instrumentation inst) {
        // 这里可以添加自定义逻辑
        System.out.println("JVM Args Injector Activated!");
    }

    public static void main(String[] args) throws Exception {
        // 添加默认 JVM 参数
        String[] jvmArgs = {
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/java.security=ALL-UNNAMED",
                "--add-opens=java.base/sun.security.rsa=ALL-UNNAMED",
                "--add-opens=java.base/sun.security.x509=ALL-UNNAMED",
                "--add-opens=java.base/sun.security.util=ALL-UNNAMED",
                "--add-opens=java.xml/com.sun.org.apache.xerces.internal.jaxp.datatype=ALL-UNNAMED",
                "-XX:+UnlockExperimentalVMOptions",
                "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
                "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
                "--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED",
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
                "--add-opens=java.base/java.io=ALL-UNNAMED",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/java.time=ALL-UNNAMED",
                "--add-modules=java.se",
                "--add-opens=java.management/sun.management=ALL-UNNAMED",
                "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED",
                "--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED",
                "--add-exports",
                "jdk.naming.dns/com.sun.jndi.dns=java.naming",
                "--add-exports",
                "java.base/sun.misc=ALL-UNNAMED"
        };

        // 构建新命令
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + "/bin/java";
        String classpath = System.getProperty("java.class.path");
        String mainClass = "io.tapdata.test.connector.ConnectorTesterMain";

        String[] command = new String[jvmArgs.length + 4];
        command[0] = javaBin;
        System.arraycopy(jvmArgs, 0, command, 1, jvmArgs.length);
        command[jvmArgs.length + 1] = "-cp";
        command[jvmArgs.length + 2] = classpath;
        command[jvmArgs.length + 3] = mainClass;

        // 启动新进程
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.inheritIO();
        Process process = builder.start();
        process.waitFor();
        System.exit(process.exitValue());
    }
}

package io.tapdata.test.connector;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 连接器测试器主类
 * 提供命令行界面来测试连接器性能
 *
 * @author TapData
 */
public class ConnectorTesterMain {

    private static final Log logger = new TapLog();
    private static final HotLoadConnectorTester tester = new HotLoadConnectorTester();
    private static final Scanner scanner = new Scanner(System.in);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public static void main(String[] args) throws Throwable {
        // 设置日志级别（如果没有通过logback配置）
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        System.out.println("=== TapData Connector Performance Tester ===");
        System.out.println("Type 'help' for available commands");

        // 检查是否启用了详细日志
        String logConfig = System.getProperty("logback.configurationFile", "");
        if (logConfig.contains("simple")) {
            System.out.println("Note: Simple logging mode enabled. Use --verbose for detailed logs.");
        }

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            tester.shutdown();
            scheduler.shutdown();
        }));

        // 主循环
        while (true) {
            System.out.print("\ntester> ");
            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                continue;
            }

            String[] parts = input.split("\\s+");
            String command = parts[0].toLowerCase();

            try {
                switch (command) {
                    case "help":
                        showHelp();
                        break;
                    case "load":
                        handleLoadCommand(parts);
                        break;
                    case "unload":
                        handleUnloadCommand(parts);
                        break;
                    case "list":
                        tester.listConnectors();
                        break;
                    case "test-connection":
                        handleTestConnectionCommand(parts);
                        break;
                    case "test-batch-read":
                        handleTestBatchReadCommand(parts);
                        break;
                    case "test-stream-read":
                        handleTestStreamReadCommand(parts);
                        break;
                    case "invoke":
                        handleInvokeCommand(parts);
                        break;
                    case "methods":
                        handleMethodsCommand(parts);
                        break;
                    case "test-write":
                        handleTestWriteCommand(parts);
                        break;
                    case "benchmark":
                        handleBenchmarkCommand(parts);
                        break;
                    case "monitor":
                        handleMonitorCommand(parts);
                        break;
                    case "exit":
                    case "quit":
                        System.out.println("Goodbye!");
                        tester.shutdown();
                        System.exit(0);
                        break;
                    default:
                        System.out.println("Unknown command: " + command + ". Type 'help' for available commands.");
                        break;
                }
            } catch (Exception e) {
                System.err.println("Error executing command: " + e.getMessage());
                logger.error("Command execution error", e);
            }
        }
    }

    private static void showHelp() {
        System.out.println("\nAvailable commands:");
        System.out.println("  load <connector-id> <jar-path> [config-file]  - Load a connector");
        System.out.println("  unload <connector-id>                         - Unload a connector");
        System.out.println("  list                                          - List loaded connectors");
        System.out.println("  methods <connector-id>                        - List connector methods");
        System.out.println("  invoke <connector-id> <method> [args...]      - Invoke connector method");
        System.out.println("  test-connection <connector-id>                - Test connector connection");
        System.out.println("  test-batch-read <connector-id> <table> [batch-size] [max-records] - Test batch read");
        System.out.println("  test-stream-read <connector-id> <table> [duration-ms] - Test stream read");
        System.out.println("  test-write <connector-id> <table> [record-count] - Test write performance");
        System.out.println("  benchmark <connector-id> <table>              - Run comprehensive benchmark");
        System.out.println("  monitor <connector-id> [interval-seconds]     - Monitor connector performance");
        System.out.println("  help                                          - Show this help");
        System.out.println("  exit/quit                                     - Exit the program");
    }

    private static void handleLoadCommand(String[] parts) throws Throwable {
        if (parts.length < 3) {
            System.out.println("Usage: load <connector-id> <jar-path> [config-file]");
            return;
        }

        String connectorId = parts[1];
        String jarPath = parts[2];

        HotLoadConnectorTester.ConnectorInfo info;
        if (parts.length > 3) {
            // 从文件加载配置
            info = tester.loadConnector(connectorId, jarPath, parts[3]);
        } else {
            // 使用默认配置
            io.tapdata.entity.utils.DataMap config = getDefaultConfig();
            info = tester.loadConnector(connectorId, jarPath, config);
        }

        System.out.println("Connector loaded: " + connectorId);
        System.out.println("Class: " + info.getConnectorClass().getName());
    }

    private static void handleUnloadCommand(String[] parts) {
        if (parts.length < 2) {
            System.out.println("Usage: unload <connector-id>");
            return;
        }

        String connectorId = parts[1];
        tester.unloadConnector(connectorId);
        System.out.println("Connector unloaded: " + connectorId);
    }

    private static void handleTestConnectionCommand(String[] parts) throws Throwable {
        if (parts.length < 2) {
            System.out.println("Usage: test-connection <connector-id>");
            return;
        }

        String connectorId = parts[1];
        System.out.println("Testing connection for: " + connectorId);

        HotLoadConnectorTester.PerformanceResult result = tester.testConnection(connectorId);
        System.out.println("Result: " + result);
    }

    private static void handleTestBatchReadCommand(String[] parts) throws Exception {
        if (parts.length < 3) {
            System.out.println("Usage: test-batch-read <connector-id> <table> [batch-size] [max-records]");
            return;
        }

        String connectorId = parts[1];
        String tableName = parts[2];
        int batchSize = parts.length > 3 ? Integer.parseInt(parts[3]) : 1000;
        int maxRecords = parts.length > 4 ? Integer.parseInt(parts[4]) : 10000;

        System.out.println("Testing batch read for: " + connectorId + "." + tableName);
        System.out.println("Batch size: " + batchSize + ", Max records: " + maxRecords);

        HotLoadConnectorTester.PerformanceResult result = tester.testBatchRead(connectorId, tableName, batchSize, maxRecords);
        System.out.println("Result: " + result);
    }

    private static void handleTestStreamReadCommand(String[] parts) throws Exception {
        if (parts.length < 3) {
            System.out.println("Usage: test-stream-read <connector-id> <table> [duration-ms]");
            return;
        }

        String connectorId = parts[1];
        String tableName = parts[2];
        int duration = parts.length > 3 ? Integer.parseInt(parts[3]) : 30000; // 30秒

        System.out.println("Testing stream read for: " + connectorId + "." + tableName);
        System.out.println("Duration: " + duration + " ms");

        HotLoadConnectorTester.PerformanceResult result = tester.testStreamRead(connectorId, Arrays.asList(tableName), duration);
        System.out.println("Result: " + result);
    }

    private static void handleMethodsCommand(String[] parts) throws Exception {
        if (parts.length < 2) {
            System.out.println("Usage: methods <connector-id>");
            return;
        }

        String connectorId = parts[1];
        System.out.println("Available methods for connector: " + connectorId);

        List<HotLoadConnectorTester.MethodInfo> methods = tester.getConnectorMethods(connectorId);
        if (methods.isEmpty()) {
            System.out.println("  No methods found");
        } else {
            for (HotLoadConnectorTester.MethodInfo method : methods) {
                System.out.println("  " + method);
            }
        }
    }

    private static void handleInvokeCommand(String[] parts) throws Exception {
        if (parts.length < 3) {
            System.out.println("Usage: invoke <connector-id> <method> [args...]");
            System.out.println("Examples:");
            System.out.println("  invoke mysql-conn connect");
            System.out.println("  invoke mysql-conn batchRead users 1000");
            System.out.println("  invoke mysql-conn test");
            return;
        }

        String connectorId = parts[1];
        String methodName = parts[2];

        // 解析参数
        Object[] args = new Object[parts.length - 3];
        for (int i = 3; i < parts.length; i++) {
            String arg = parts[i];
            // 简单的类型推断
            if (arg.matches("\\d+")) {
                args[i - 3] = Integer.parseInt(arg);
            } else if (arg.matches("\\d+\\.\\d+")) {
                args[i - 3] = Double.parseDouble(arg);
            } else if (arg.equalsIgnoreCase("true") || arg.equalsIgnoreCase("false")) {
                args[i - 3] = Boolean.parseBoolean(arg);
            } else {
                args[i - 3] = arg;
            }
        }

        System.out.println("Invoking method: " + methodName + " on connector: " + connectorId);
        if (args.length > 0) {
            System.out.println("Arguments: " + Arrays.toString(args));
        }

        try {
            Object result = tester.invokeConnectorMethod(connectorId, methodName, args);
            System.out.println("Result: " + result);
        } catch (Exception e) {
            System.err.println("Method invocation failed: " + e.getMessage());
            logger.error("Method invocation error", e);
        }
    }

    private static List<Map<String, Object>> generateTestRecords(int recordCount) {
        return null;
    }

    private static void handleBenchmarkCommand(String[] parts) throws Throwable {
        if (parts.length < 3) {
            System.out.println("Usage: benchmark <connector-id> <table>");
            return;
        }

        String connectorId = parts[1];
        String tableName = parts[2];

        System.out.println("Running comprehensive benchmark for: " + connectorId + "." + tableName);
        System.out.println("This may take several minutes...");

        // 运行多个测试
        System.out.println("\n1. Testing connection...");
        HotLoadConnectorTester.PerformanceResult connectionResult = tester.testConnection(connectorId);
        System.out.println("  " + connectionResult);

        System.out.println("\n2. Testing batch read (small batch)...");
        HotLoadConnectorTester.PerformanceResult batchSmall = tester.testBatchRead(connectorId, tableName, 100, 1000);
        System.out.println("  " + batchSmall);

        System.out.println("\n3. Testing batch read (large batch)...");
        HotLoadConnectorTester.PerformanceResult batchLarge = tester.testBatchRead(connectorId, tableName, 1000, 10000);
        System.out.println("  " + batchLarge);

        System.out.println("\n4. Testing stream read...");
        HotLoadConnectorTester.PerformanceResult stream = tester.testStreamRead(connectorId, Arrays.asList(tableName), 10000);
        System.out.println("  " + stream);

        System.out.println("\nBenchmark completed!");
    }

    private static void handleMonitorCommand(String[] parts) {
        if (parts.length < 2) {
            System.out.println("Usage: monitor <connector-id> [interval-seconds]");
            return;
        }

        String connectorId = parts[1];
        int interval = parts.length > 2 ? Integer.parseInt(parts[2]) : 5;

        System.out.println("Starting monitoring for: " + connectorId + " (interval: " + interval + "s)");
        System.out.println("Press Enter to stop monitoring...");

        // 启动监控任务
        scheduler.scheduleAtFixedRate(() -> {
            try {
                HotLoadConnectorTester.PerformanceResult result = tester.testConnection(connectorId);
                System.out.println("[" + new Date() + "] " + result);
            } catch (Throwable e) {
                System.out.println("[" + new Date() + "] Monitor error: " + e.getMessage());
            }
        }, 0, interval, TimeUnit.SECONDS);

        // 等待用户输入停止
        scanner.nextLine();
        System.out.println("Monitoring stopped.");
    }

    private static io.tapdata.entity.utils.DataMap getDefaultConfig() {
        io.tapdata.entity.utils.DataMap config = new io.tapdata.entity.utils.DataMap();
        config.put("host", "localhost");
        config.put("port", 3306);
        config.put("database", "test");
        config.put("username", "root");
        config.put("password", "password");
        return config;
    }

    private static void handleTestWriteCommand(String[] parts) throws Exception {
        if (parts.length < 3) {
            System.out.println("Usage: test-write <connector-id> <table> [record-count]");
            return;
        }

        String connectorId = parts[1];
        String tableName = parts[2];
        int recordCount = parts.length > 3 ? Integer.parseInt(parts[3]) : 1000;

        System.out.println("Testing write for: " + connectorId + "." + tableName);
        System.out.println("Record count: " + recordCount);

        // 生成测试数据
        List<TapRecordEvent> events = tester.generateTestEvents(recordCount);

        HotLoadConnectorTester.PerformanceResult result = tester.testWriteRecord(connectorId, tableName, events);
        System.out.println("Result: " + result);
    }
}

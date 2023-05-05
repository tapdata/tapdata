package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * 连接测试线程泄露检查
 * 测试失败按警告上报
 */
@DisplayName("checkThreadTest")
@TapGo(tag = "V3", sort = 10060, debug = false)
public class ConnectionTestThreadTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("checkThreadTest.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(supportAny(
                langUtil.formatLang(anyOneFunFormat, "QueryByAdvanceFilterFunction,QueryByFilterFunction"),
                QueryByAdvanceFilterFunction.class, QueryByFilterFunction.class),
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction"))
        );
    }

    /**
     * 用例1，单线程多次执行连接测试后检查线程数量
     * 单线程执行测试连接方法5次， 测试前记录线程列表和数量，
     * 测试后再检查线程列表和数量，
     * 线程应该回到测试前的数量，
     * 如果有问题， 找出多出来的线程栈， 并输出
     */
    @DisplayName("checkThreadTest.alone")
    @TapTestCase(sort = 1)
    @Test
    public void alone() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("checkThreadTest.alone.wait"));
        final int connectionTestTimes = 5;
        super.execTestConnection((node, testCase) -> {
            StringJoiner beforeBuilder = new StringJoiner("\n");
            List<StackEntity> before = this.printThreads(beforeBuilder);
            this.connectionTest(node, connectionTestTimes, Boolean.FALSE);
            StringJoiner afterBuilder = new StringJoiner("\n");
            List<StackEntity> after = this.printThreads(afterBuilder);
            String different = this.differentStack(before, after);
            if (!"".equals(different.trim())) {
                TapAssert.warn(testCase, langUtil.formatLang(
                        "checkThreadTest.alone.fail",
                        connectionTestTimes,
                        before.size(),
                        beforeBuilder.toString() + "；\n" + LangUtil.SPILT_GRADE_4 + "",
                        after.size(),
                        afterBuilder.toString() + "，\n" + LangUtil.SPILT_GRADE_4 + "",
                        different));
            } else {
                TapAssert.succeed(testCase, langUtil.formatLang(
                        "checkThreadTest.alone.succeed",
                        connectionTestTimes,
                        before.size(),
                        beforeBuilder.toString() + "；\n" + LangUtil.SPILT_GRADE_4 + "",
                        after.size(),
                        afterBuilder.toString() + "，\n" + LangUtil.SPILT_GRADE_4 + ""));
            }
        });
    }

    Set<String> currentThreads = new ConcurrentSkipListSet<>();

    /**
     * 用例2，多线程多次执行连接测试后检查线程数量
     * 开启5个线程线程执行测试连接方法各1次，
     * 测试前记录线程列表和数量，
     * 测试后再检查线程列表和数量，
     * 线程应该回到测试前的数量，
     * 如果有问题， 找出多出来的线程栈， 并输出
     */
    @DisplayName("checkThreadTest.multi")
    @TapTestCase(sort = 2)
    @Test
    public void multi() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("checkThreadTest.multi.wait"));
        final int connectionTestThread = 5;
        final int connectionTestTimes = 1;
        ExecutorService service = Executors.newFixedThreadPool(connectionTestThread);
        AtomicInteger awaitTimes = new AtomicInteger(connectionTestThread);
        final Object lock = new Object();
        super.execTestConnection((node, testCase) -> {
            StringJoiner beforeBuilder = new StringJoiner("\n");
            List<StackEntity> before = this.printThreads(beforeBuilder);

            for (int index = 0; index < connectionTestThread; index++) {
                service.execute(() -> {
                    currentThreads.add(Thread.currentThread().getName());
                    try {
                        this.connectionTest(node, connectionTestTimes, Boolean.TRUE);
                    } finally {
                        synchronized (lock) {
                            awaitTimes.decrementAndGet();
                        }
                    }
                });
            }
            while (awaitTimes.get() > 0) {
                try {
                    synchronized (lock) {
                        lock.wait(1000);
                    }
                } catch (InterruptedException ignored) {
                }
            }
            StringJoiner afterBuilder = new StringJoiner("\n");
            List<StackEntity> after = this.printThreads(afterBuilder);
            String different = this.differentStack(before, after);
            if (!"".equals(different.trim())) {
                TapAssert.warn(testCase, langUtil.formatLang(
                        "checkThreadTest.multi.fail",
                        connectionTestThread,
                        connectionTestTimes,
                        before.size(),
                        beforeBuilder.toString() + "；\n" + LangUtil.SPILT_GRADE_4 + "",
                        after.size(),
                        afterBuilder.toString() + "，\n" + LangUtil.SPILT_GRADE_4 + "",
                        different));
            } else {
                TapAssert.succeed(testCase, langUtil.formatLang(
                        "checkThreadTest.multi.succeed",
                        connectionTestThread,
                        connectionTestTimes,
                        before.size(),
                        beforeBuilder.toString() + "；\n" + LangUtil.SPILT_GRADE_4 + "",
                        after.size(),
                        afterBuilder.toString() + "，\n" + LangUtil.SPILT_GRADE_4 + ""));
            }
        }, (node, testCase) -> {
            service.shutdown();
            lock.notifyAll();
            currentThreads = null;
        });
    }

    private String differentStack(List<StackEntity> before, List<StackEntity> after) {
        StringJoiner builder = new StringJoiner("\n");
        for (int index = after.size() - 1; index >= 0; index--) {
            boolean equals = true;
            StackEntity stackEntity = after.get(index);
            for (StackEntity entity : before) {
                equals = entity.name().equals(stackEntity.name());
                if (equals)  break;
            }
            if (!equals && !currentThreads.contains(stackEntity.name())) {
                builder.add(LangUtil.SPILT_GRADE_4 + LangUtil.SPILT_GRADE_2 + "[ thread: (" + stackEntity.name() + ") | state: (" + stackEntity.state() + ") ]");
            }
        }
        return builder.toString();
    }

    private List<StackEntity> printThreads(StringJoiner builder) {
        List<StackEntity> stackEntities = new ArrayList<>();
        Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
        threadMap.forEach((thread, stackTraceElements) -> {
            String threadName = thread.getName();
            if (!currentThreads.contains(threadName)) {
                builder.add(LangUtil.SPILT_GRADE_4 + LangUtil.SPILT_GRADE_2 + "[ thread: (" + threadName + ") | state: (" + thread.getState().name() + ") ]");
                stackEntities.add(StackEntity.create(threadName, thread.getState().name()));
            }
        });
        return stackEntities;
    }

    private void connectionTest(TestNode prepare, final int execTimes, boolean isMulti) {
        TapConnector connector = prepare.connectorNode().getConnector();
        for (int index = 0; index < execTimes; index++) {
            try {
                connector.connectionTest(prepare.connectorNode().getConnectorContext(), testItem -> {
                });
            } catch (Throwable throwable) {
                TapAssert.error(
                        prepare.recordEventExecute().testCase(),
                        langUtil.formatLang(isMulti ?
                                        "checkThreadTest.connection.multi.throw" :
                                        "checkThreadTest.connection.alone.throw",
                                index + 1,
                                isMulti ? Thread.currentThread().getName() : "",
                                throwable.getMessage()));
            }
        }
    }
}

class StackEntity {
    String name;
    String state;

    public String name() {
        return this.name;
    }

    public String state() {
        return this.state;
    }

    public StackEntity name(String name) {
        this.name = name;
        return this;
    }

    public StackEntity state(String state) {
        this.state = state;
        return this;
    }

    public static StackEntity create(String name, String state) {
        return new StackEntity().name(name).state(state);
    }
}
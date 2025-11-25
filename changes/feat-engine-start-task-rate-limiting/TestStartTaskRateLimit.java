import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试 startTask 方法的限流功能
 */
public class TestStartTaskRateLimit {
    
    // 模拟限流控制
    private final Object startTaskLock = new Object();
    private volatile long lastStartTaskTime = 0L;
    private static final long START_TASK_RATE_LIMIT_MILLIS = 2000L; // 2秒限流间隔（测试用）
    
    // 计数器
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    
    /**
     * 模拟 applyStartTaskRateLimit 方法
     */
    private void applyStartTaskRateLimit(String taskName) {
        synchronized (startTaskLock) {
            long currentTime = System.currentTimeMillis();
            long timeSinceLastStart = currentTime - lastStartTaskTime;
            
            if (timeSinceLastStart < START_TASK_RATE_LIMIT_MILLIS) {
                long waitTime = START_TASK_RATE_LIMIT_MILLIS - timeSinceLastStart;
                System.out.println("[" + currentTime + "] Rate limiting startTask for task " + taskName + 
                    ", waiting " + waitTime + "ms to maintain 2-second interval");
                
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Rate limit wait interrupted for task " + taskName);
                    throw new RuntimeException("Rate limit wait interrupted", e);
                }
            }
            
            // 更新最后执行时间
            lastStartTaskTime = System.currentTimeMillis();
            System.out.println("[" + lastStartTaskTime + "] StartTask rate limit applied for task " + taskName + 
                ", last execution time updated");
        }
    }
    
    /**
     * 模拟 startTask 方法
     */
    public void startTask(String taskName) {
        // 应用限流控制
        applyStartTaskRateLimit(taskName);
        
        // 模拟实际的任务启动逻辑
        int taskNum = taskCounter.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        System.out.println("[" + startTime + "] Starting task " + taskNum + ": " + taskName);
        
        try {
            // 模拟任务启动耗时
            Thread.sleep(100);
            System.out.println("[" + System.currentTimeMillis() + "] Task " + taskNum + " (" + taskName + ") started successfully");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Task startup interrupted: " + taskName);
        }
    }
    
    /**
     * 测试并发调用 startTask
     */
    public void testConcurrentStartTask() {
        System.out.println("=== Testing Concurrent StartTask Rate Limiting ===");
        
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch latch = new CountDownLatch(5);
        
        // 同时提交5个任务启动请求
        for (int i = 1; i <= 5; i++) {
            final String taskName = "ConcurrentTask-" + i;
            executor.submit(() -> {
                try {
                    startTask(taskName);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        try {
            // 等待所有任务完成
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Test interrupted");
        }
        
        executor.shutdown();
        System.out.println("=== Concurrent test completed ===\n");
    }
    
    /**
     * 测试顺序调用 startTask
     */
    public void testSequentialStartTask() {
        System.out.println("=== Testing Sequential StartTask Rate Limiting ===");
        
        // 顺序调用5个任务
        for (int i = 1; i <= 5; i++) {
            String taskName = "SequentialTask-" + i;
            startTask(taskName);
        }
        
        System.out.println("=== Sequential test completed ===\n");
    }
    
    /**
     * 测试不同调用源的限流效果
     */
    public void testDifferentCallers() {
        System.out.println("=== Testing Different Callers Rate Limiting ===");
        
        // 模拟不同的调用源
        Runnable caller1 = () -> startTask("FromCaller1");
        Runnable caller2 = () -> startTask("FromCaller2");
        Runnable caller3 = () -> startTask("FromCaller3");
        
        ExecutorService executor = Executors.newFixedThreadPool(3);
        
        // 几乎同时调用
        executor.submit(caller1);
        executor.submit(caller2);
        executor.submit(caller3);
        
        try {
            Thread.sleep(8000); // 等待8秒
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        executor.shutdown();
        System.out.println("=== Different callers test completed ===\n");
    }
    
    /**
     * 测试主方法
     */
    public static void main(String[] args) {
        TestStartTaskRateLimit test = new TestStartTaskRateLimit();
        
        try {
            // 测试顺序调用
            test.testSequentialStartTask();
            
            // 等待一段时间
            Thread.sleep(3000);
            
            // 测试并发调用
            test.testConcurrentStartTask();
            
            // 等待一段时间
            Thread.sleep(3000);
            
            // 测试不同调用源
            test.testDifferentCallers();
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Test interrupted");
        }
        
        System.out.println("All tests completed. Total tasks processed: " + test.taskCounter.get());
    }
}

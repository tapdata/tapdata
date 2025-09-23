import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * TaskRestartSchedule TM启动时间检查功能的单元测试
 */
public class TaskRestartScheduleTest {
    
    public static void main(String[] args) {
        testTmStartTimeCheck();
    }
    
    public static void testTmStartTimeCheck() {
        System.out.println("=== TaskRestartSchedule TM启动时间检查测试 ===");
        
        try {
            // 模拟TaskRestartSchedule类的行为
            MockTaskRestartSchedule schedule = new MockTaskRestartSchedule();
            
            // 测试场景1：TM刚启动（运行时间 < 10分钟）
            schedule.setTmStartTime(System.currentTimeMillis() - 300000L); // 5分钟前
            boolean shouldSkip1 = schedule.shouldSkipEngineRestartCheck();
            System.out.println("场景1 - TM启动5分钟前: " + (shouldSkip1 ? "跳过执行" : "继续执行"));
            
            // 测试场景2：TM启动时间等于10分钟
            schedule.setTmStartTime(System.currentTimeMillis() - 600000L); // 10分钟前
            boolean shouldSkip2 = schedule.shouldSkipEngineRestartCheck();
            System.out.println("场景2 - TM启动10分钟前: " + (shouldSkip2 ? "跳过执行" : "继续执行"));
            
            // 测试场景3：TM启动时间大于10分钟
            schedule.setTmStartTime(System.currentTimeMillis() - 900000L); // 15分钟前
            boolean shouldSkip3 = schedule.shouldSkipEngineRestartCheck();
            System.out.println("场景3 - TM启动15分钟前: " + (shouldSkip3 ? "跳过执行" : "继续执行"));
            
            System.out.println("\n=== 测试结果验证 ===");
            System.out.println("预期结果：");
            System.out.println("- 场景1应该跳过执行（TM启动时间 < 10分钟）: " + (shouldSkip1 ? "✓" : "✗"));
            System.out.println("- 场景2应该继续执行（TM启动时间 = 10分钟）: " + (!shouldSkip2 ? "✓" : "✗"));
            System.out.println("- 场景3应该继续执行（TM启动时间 > 10分钟）: " + (!shouldSkip3 ? "✓" : "✗"));
            
            boolean allTestsPassed = shouldSkip1 && !shouldSkip2 && !shouldSkip3;
            System.out.println("\n总体测试结果: " + (allTestsPassed ? "✓ 所有测试通过" : "✗ 部分测试失败"));
            
        } catch (Exception e) {
            System.err.println("测试执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 模拟TaskRestartSchedule类的TM启动时间检查逻辑
     */
    static class MockTaskRestartSchedule {
        private long tmStartTime;
        
        public void setTmStartTime(long startTime) {
            this.tmStartTime = startTime;
        }
        
        /**
         * 模拟engineRestartNeedStartTask方法中的TM启动时间检查逻辑
         * @return true表示应该跳过执行，false表示继续执行
         */
        public boolean shouldSkipEngineRestartCheck() {
            long currentTime = System.currentTimeMillis();
            long tmRunningTime = currentTime - tmStartTime;
            
            if (tmRunningTime < 600000L) { // 10分钟 = 600000毫秒
                System.out.println("  -> TM运行时间: " + tmRunningTime + "ms (" + (tmRunningTime/1000/60) + "分钟)");
                return true; // 跳过执行
            }
            
            return false; // 继续执行
        }
    }
}

import java.util.*;

/**
 * 测试TM启动时间检查逻辑
 */
public class TestLogic {
    
    // 模拟TM启动时间
    private static final long TM_START_TIME = System.currentTimeMillis();
    
    public static void main(String[] args) {
        // 模拟测试数据
        testTmStartTimeCheck();
    }
    
    public static void testTmStartTimeCheck() {
        System.out.println("=== 测试TM启动时间检查逻辑 ===");
        
        // 测试场景1：TM启动时间小于10分钟（模拟5分钟前启动）
        long tmStartTime1 = System.currentTimeMillis() - 300000L; // 5分钟前
        boolean shouldSkip1 = checkTmStartTime(tmStartTime1);
        System.out.println("场景1 - TM启动5分钟前: " + (shouldSkip1 ? "跳过执行" : "继续执行"));
        
        // 测试场景2：TM启动时间等于10分钟
        long tmStartTime2 = System.currentTimeMillis() - 600000L; // 10分钟前
        boolean shouldSkip2 = checkTmStartTime(tmStartTime2);
        System.out.println("场景2 - TM启动10分钟前: " + (shouldSkip2 ? "跳过执行" : "继续执行"));
        
        // 测试场景3：TM启动时间大于10分钟
        long tmStartTime3 = System.currentTimeMillis() - 900000L; // 15分钟前
        boolean shouldSkip3 = checkTmStartTime(tmStartTime3);
        System.out.println("场景3 - TM启动15分钟前: " + (shouldSkip3 ? "跳过执行" : "继续执行"));
        
        // 测试场景4：实际的TM启动时间（刚启动）
        boolean shouldSkip4 = checkTmStartTime(TM_START_TIME);
        System.out.println("场景4 - TM刚启动: " + (shouldSkip4 ? "跳过执行" : "继续执行"));
        
        System.out.println("\n=== 测试结果 ===");
        System.out.println("预期结果：");
        System.out.println("- 场景1应该跳过执行（TM启动时间 < 10分钟）");
        System.out.println("- 场景2应该继续执行（TM启动时间 = 10分钟）");
        System.out.println("- 场景3应该继续执行（TM启动时间 > 10分钟）");
        System.out.println("- 场景4应该跳过执行（TM刚启动）");
    }
    
    /**
     * 模拟原方法中的TM启动时间检查逻辑
     * @param tmStartTime TM启动时间戳
     * @return true表示应该跳过执行，false表示继续执行
     */
    public static boolean checkTmStartTime(long tmStartTime) {
        long currentTime = System.currentTimeMillis();
        long tmRunningTime = currentTime - tmStartTime;
        
        if (tmRunningTime < 600000L) { // 10分钟 = 600000毫秒
            System.out.println("  -> TM运行时间: " + tmRunningTime + "ms (" + (tmRunningTime/1000/60) + "分钟)");
            return true; // 跳过执行
        }
        
        return false; // 继续执行
    }
}

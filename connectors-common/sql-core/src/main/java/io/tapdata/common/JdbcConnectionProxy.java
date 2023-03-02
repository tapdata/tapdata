package io.tapdata.common;

import io.tapdata.entity.logger.TapLogger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * jdbc connection 代理类，用于在创建jdbc连接时，加入自定义逻辑
 *
 * @author jackin
 * @date 2021/5/23 11:39 PM
 **/
public class JdbcConnectionProxy implements InvocationHandler {
	private final static String TAG = JdbcConnectionProxy.class.getSimpleName();

  private Connection connection;

  /**
   * 线程方法调用栈的toString后生成的hash code
   */
  private String threadCallStackHashCode;

  /**
   * 记录已创建的jdbc的连接数
   * key：线程方法调用栈的toString后生成的hash code
   * value： 连接数统计数
   */
  private static ConcurrentHashMap<String, AtomicInteger> aliveConnectionCount = new ConcurrentHashMap<>();

//  /**
//   * 记录每个jdbc connection对应 线程方法调用栈的toString后 的hash code
//   * key：this
//   * value：线程方法调用栈的toString后生成的hash code
//   */
//  private static ConcurrentHashMap<Connection, String> aliveConnectionHashCode = new ConcurrentHashMap<>();

  public JdbcConnectionProxy(Connection connection) {
    this.connection = connection;

    final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    StringBuilder sb = new StringBuilder();
    if (stackTrace != null && stackTrace.length > 0) {
      for (StackTraceElement stackTraceElement : stackTrace) {
        sb.append(stackTraceElement.toString());
      }

      this.threadCallStackHashCode = String.valueOf(sb.toString().hashCode());

      synchronized (this.threadCallStackHashCode) {
        if (!aliveConnectionCount.containsKey(this.threadCallStackHashCode)) {
          aliveConnectionCount.put(this.threadCallStackHashCode, new AtomicInteger(0));
        }

        aliveConnectionCount.get(this.threadCallStackHashCode).incrementAndGet();

//        aliveConnectionHashCode.put(connection, hashCode);

        final int aliveCount = aliveConnectionCount.get(this.threadCallStackHashCode).get();
        if (aliveCount > 1 && aliveCount % 2 == 0) {
          TapLogger.debug(TAG, "Alive jdbc connection threshold warning, alive count {}, call stack {}.", aliveCount, sb.toString());
        }
      }
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    final String methodName = method.getName();
    if ("close".equals(methodName)) {

      try {
        if (aliveConnectionCount.containsKey(threadCallStackHashCode)) {
          int aliveCount = aliveConnectionCount.get(threadCallStackHashCode).decrementAndGet();
          if (aliveCount <= 0) {
            synchronized (threadCallStackHashCode.intern()) {
              aliveCount = aliveConnectionCount.getOrDefault(threadCallStackHashCode, new AtomicInteger(0)).get();
              if (aliveCount <= 0) {
                aliveConnectionCount.remove(threadCallStackHashCode);
              }
            }
          }
        }
      } catch (Exception e) {
		  TapLogger.warn(TAG, "JdbcConnectionProxy close exception {}, stack {}.", e.getMessage(), e.getStackTrace());
      }

    }
    return method.invoke(connection, args);
  }
}

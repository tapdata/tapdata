package com.tapdata.tm.lock.annotation;

import com.tapdata.tm.lock.constant.LockType;
import org.aspectj.lang.ProceedingJoinPoint;

import java.lang.annotation.*;

/**
 * @Author: Zed
 * @Date: 2021/12/17
 * @Description: 利用mongodb的findandmodify实现的分布式锁，
 *                  目前不是一个可重入锁。有需求可以修改
 *                  目前没有自动续期功能，所以非常严谨的需求谨慎使用
 * @see com.tapdata.tm.lock.aop.LockAop#lock(ProceedingJoinPoint) aop实现
 * @see com.tapdata.tm.lock.service.impl.LockServiceImpl mongodb实现
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Lock {
    /** 锁的key 方法参数中的值，可以用.去属性， 如@Lock(task.id) 则是取参数task对象的id作为key， 如果参数的属性为对象，默认取对象的tostring方法作为key*/
    String value();
    /** 锁的类型，不同类型的竞争可以用不同的锁  例如，任务的模型推演，跟任务的校验，是不同的类型，分别加锁不会冲突*/
    LockType type() default LockType.DEFAULT_LOCK;
    /** 过期时间 单位 秒*/
    int expireSeconds() default 10;
    /** 循环等待睡眠时间 单位毫秒*/
    int sleepMillis() default 50;
}

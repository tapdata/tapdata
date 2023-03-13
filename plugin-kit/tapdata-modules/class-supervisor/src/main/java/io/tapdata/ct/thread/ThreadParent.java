package io.tapdata.ct.thread;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadParent {
    public ThreadParent(List<Map<String,Object>> arr, Object[] a,List<Object> o){

    }
    public Void a(Object[] a){

        return null;
    }
    @Test
    public void checkThreadParent() {
        AtomicInteger count = new AtomicInteger(100);
        final Object lock = new Object();
        Thread main = Thread.currentThread();
        ThreadGroup group1 = new ThreadGroup("Coding-Group-Main");
        //group1.uncaughtException();
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.submit(() ->{
            System.out.println("");
        });
        group1.destroy();
        Thread thread1 = new Thread(group1,()->{
            Thread another = new Thread(() -> {
                System.out.println("");
                ThreadGroup group = new ThreadGroup("Coding-Group");
                Thread another1 = new Thread(group,()->{
                    System.out.println("Coding-Group-sub1 is testing!");
                },"Coding-Group-sub1");
                Thread another2 = new Thread(()->{
                    System.out.println("Codin2 is testing!");
                },"Coding-sub2");
                another1.start();
                another2.start();
            }, "aaaa");


            executorService.submit(() -> {
                System.out.println("");
            });
            another.start();

            while (count.get() >0){
                count.getAndDecrement();
                try {
                    Thread.sleep(3000);
                }catch (Exception ignore){

                }
            }
        },"Gavin-Test-Thread-Zero");

        thread1.start();
        AtomicInteger count1 = new AtomicInteger(100);
        final Object lock1 = new Object();
        while (count1.get() > 0){
            new Thread(()->{
                while (count.get() >0){
                    count.getAndDecrement();
                    try {
                        Thread.sleep(3000);
                    }catch (Exception ignore){

                    }
                }
            },"Gavin-Test-Thread-"+count1).start();
            Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
            threadMap.forEach((tdd,s)->{
                if (Objects.isNull(tdd.getUncaughtExceptionHandler()) || tdd.getThreadGroup().equals(tdd.getUncaughtExceptionHandler())) {
                    tdd.setUncaughtExceptionHandler((t, e) -> {
                        System.out.println(t.getThreadGroup().getName() + ":" + e.getMessage());
                    });
                }
                ThreadGroup threadGroup = tdd.getThreadGroup();
                if (Objects.nonNull(threadGroup)){
                    System.out.println(threadGroup.getName() + "-" +  threadGroup.activeCount());
                }
            });
            count1.getAndDecrement();
            try {
                Thread.sleep(5000);
            }catch (Exception ignore){

            }
        }
    }
}

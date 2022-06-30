//package com.tapdata.tm.monitor.servlet;
//
//import lombok.extern.slf4j.Slf4j;
//
//import javax.servlet.AsyncContext;
//import javax.servlet.ServletException;
//import javax.servlet.ServletRequest;
//import javax.servlet.ServletResponse;
//import javax.servlet.annotation.WebServlet;
//import javax.servlet.http.HttpServlet;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//
///**
// * 异步Servlet 案例
// * asyncSupported = true 表示开启异步支持
// */
//@WebServlet(name = "AsyncServlet",
//        asyncSupported = true,
//        urlPatterns = "/api/measurement/async")
//@Slf4j
//public class AsyncServlet extends HttpServlet {
//    @Override
//    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//        log.info("【doPost async Servlet 】");
//    }
//
//    @Override
//    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//        long start = System.currentTimeMillis();
//
//        //获取异步上下文
//        AsyncContext asyncContext = request.startAsync();
//
//        //使用java8的 CompletableFuture，将任务放到线程池中去执行
//        CompletableFuture.runAsync(() -> {
//            doSomeThing(asyncContext, asyncContext.getRequest(), asyncContext.getResponse());
//        });
//
//        long end = System.currentTimeMillis();
//
//        log.info("【doGet async Servlet 耗时 {} 】", (end - start));
//    }
//
//    private void doSomeThing(AsyncContext asyncContext, ServletRequest request, ServletResponse response) {
//
//        //执行业务代码
//        try {
//            log.info("执行业务代码");
//            TimeUnit.SECONDS.sleep(3);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        //告诉异步上下文 结束了
//        asyncContext.complete();
//    }
//}

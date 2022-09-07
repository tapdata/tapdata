package com.tapdata.tm.proxy.controller;

import com.tapdata.tm.async.AsyncContextManager;
import com.tapdata.tm.async.AsyncJobWaiter;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.service.CommandExecutionService;
import io.tapdata.pdk.apis.entity.CommandInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;

/**
 * 异步Servlet 案例
 * asyncSupported = true 表示开启异步支持
 */
@WebServlet(name = "AsyncServlet",
        asyncSupported = true,
        urlPatterns = "/api/proxy/command")
@Slf4j
public class CommandAsyncServlet extends HttpServlet {
    AsyncContextManager asyncContextManager = new AsyncContextManager();
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String body = IOUtils.toString(request.getInputStream(), StandardCharsets.UTF_8);
        if(StringUtils.isEmpty(body)) {
            /*
            {
                "reqId": "870382ee-1c11-4ee1-b652-3f10c10b0dbd",
                "ts": 1662487655132,
                "code": "NotLogin",
                "message": "User is not login."
            }
            {
                "reqId": "4eb7f80d-3a2b-4a21-8701-6e62807cc512",
                "ts": 1662487645602,
                "code": "ok"
            }
             */
            response.setContentType("application/json");
            String errorTemplate = "{\n" +
                    "                \"reqId\": \"" + UUID.randomUUID() + "\",\n" +
                    "                \"ts\": " + System.currentTimeMillis() + ",\n" +
                    "                \"code\": \"Illegal\",\n" +
                    "                \"message\": \"Body is empty\"\n" +
                    "            }";
            response.getOutputStream().write(errorTemplate.getBytes(StandardCharsets.UTF_8));
            return;
        }

        CommandInfo commandInfo = fromJson(body, CommandInfo.class);
        CommandExecutionService commandExecutionService = InstanceFactory.instance(CommandExecutionService.class);
        commandExecutionService.call(commandInfo, new BiConsumer<Map<String, Object>, Throwable>() {
            @Override
            public void accept(Map<String, Object> stringObjectMap, Throwable throwable) {

            }
        });
        asyncContextManager.registerAsyncJob("", request, new AsyncJobWaiter<Map<String, Object>>() {
            @Override
            public void jobAccomplished(Map<String, Object> result, Throwable error) {

            }
        });
        asyncContextManager.applyAsyncJobResult("", null, null);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        long start = System.currentTimeMillis();

        //获取异步上下文
        AsyncContext asyncContext = request.startAsync();

        //使用java8的 CompletableFuture，将任务放到线程池中去执行
        CompletableFuture.runAsync(() -> {
            doSomeThing(asyncContext, asyncContext.getRequest(), asyncContext.getResponse());
        });

        long end = System.currentTimeMillis();

        log.info("【doGet async Servlet 耗时 {} 】", (end - start));
    }

    private void doSomeThing(AsyncContext asyncContext, ServletRequest request, ServletResponse response) {

        //执行业务代码
        try {
            log.info("执行业务代码");
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //告诉异步上下文 结束了
        asyncContext.complete();
    }
}

package com.tapdata.tm.proxy.service.impl;

import com.google.common.collect.Sets;
import com.tapdata.tm.async.AsyncContextManager;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.utils.RemoteCallerUtil;
import com.tapdata.tm.worker.dto.WorkerExpireDto;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.modules.api.net.data.FileMeta;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.EngineMessageExecutionService;
import io.tapdata.pdk.apis.entity.message.EngineMessage;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

import jakarta.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.BiConsumer;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/8 10:00 Create
 * @description
 */
@Slf4j
@Service
public class RemoteCaller {
    private final AsyncContextManager asyncContextManager = new AsyncContextManager();
    @Resource(name = "productComponent")
    ProductComponent productComponent;
    @Resource(name = "workerServiceImpl")
    WorkerService workerService;

    public void callMethod(@RequestBody ServiceCaller serviceCaller, HttpServletRequest request, HttpServletResponse response, UserDetail userDetail) {
        if (serviceCaller == null)
            throw new BizException("serviceCaller is illegal");
        if (serviceCaller.getClassName() == null)
            throw new BizException("Missing className");
        if (serviceCaller.getMethod() == null)
            throw new BizException("Missing method");

        if (productComponent.isCloud()) {
            serviceCaller.subscribeIds("userId_" + userDetail.getUserId());
            WorkerExpireDto shareWorker = workerService.getShareWorker(userDetail);
            if (shareWorker != null) {
                serviceCaller.orSubscribeIdSets(Sets.newHashSet("userId_" + shareWorker.getShareTmUserId()));
            }
        }
        executeServiceCaller(request, response, serviceCaller, userDetail);
    }

    public void executeServiceCaller(HttpServletRequest request, HttpServletResponse response, ServiceCaller serviceCaller, UserDetail userDetail) {
        serviceCaller.setId(UUID.randomUUID().toString().replace("-", ""));
        if (StringUtils.isBlank(serviceCaller.getReturnClass()))
            serviceCaller.setReturnClass(Object.class.getName());
        Object[] args = serviceCaller.getArgs();
        DataMap context = null;
        if (userDetail != null)
            context = DataMap.create().kv("userId", userDetail.getUserId()).kv("customerId", userDetail.getCustomerId());
        if (args == null) {
            serviceCaller.setArgs(new Object[]{context});
        } else {
            Object[] newArgs = new Object[args.length + 1];
            System.arraycopy(args, 0, newArgs, 0, args.length);
            newArgs[args.length] = context;
            serviceCaller.setArgs(newArgs);
        }
        executeEngineMessage(serviceCaller, request, response);
    }

    public void executeEngineMessage(EngineMessage engineMessage, HttpServletRequest request, HttpServletResponse response) {
        EngineMessageExecutionService engineMessageExecutionService = getEngineMessageExecutionService();
        registerAsyncJob(engineMessage.getId(), request, response);
        try {
            engineMessageExecutionService.call(engineMessage, (result, throwable) -> asyncContextManager.applyAsyncJobResult(engineMessage.getId(), result, throwable));
        } catch (Exception throwable) {
            asyncContextManager.applyAsyncJobResult(engineMessage.getId(), null, throwable);
        }
    }

    private void executeEngineMessage(EngineMessage engineMessage, BiConsumer<Object, Throwable> resultCallback) {
        EngineMessageExecutionService engineMessageExecutionService = getEngineMessageExecutionService();
        try {
            engineMessageExecutionService.call(engineMessage, resultCallback);
        } catch (Exception throwable) {
            throw new CoreException(String.format("Error executing engineMessage: %s", engineMessage), throwable);
        }
    }

    private void registerAsyncJob(String id, HttpServletRequest request, HttpServletResponse response) {
        asyncContextManager.registerAsyncJob(id, request, (result, error) -> {
            String responseStr;
            if (error != null) {
                int code = NetErrors.UNKNOWN_ERROR;
                Object data = null;
                if (error instanceof CoreException core) {
                    code = core.getCode();
                    data = core.getData();
                }
                responseStr =
                        "{\n" +
                                "    \"reqId\": \"" + UUID.randomUUID() + "\",\n" +
                                "    \"ts\": " + System.currentTimeMillis() + ",\n" +
                                "    \"code\": \"" + code + "\",\n" +
                                "    \"message\": \"" + error.getMessage() + "\"" +
                                (null != data ? ",\n    \"data\": " + toJson(data, JsonParser.ToJsonFeature.PrettyFormat) + "\n" : "\n") +
                                "}";
            } else {
                responseStr =
                        "{\n" +
                                "    \"reqId\": \"" + UUID.randomUUID() + "\",\n" +
                                "    \"ts\": " + System.currentTimeMillis() + ",\n" +
                                "    \"data\": " + (result != null ? toJson(result, JsonParser.ToJsonFeature.PrettyFormat) : "{}") + ",\n" +
                                "    \"code\": \"ok\"\n" +
                                "}";
            }
            afterRegisterAsyncJob(request, response, responseStr);
        });
    }

    protected void afterRegisterAsyncJob(Object result, HttpServletResponse response, String responseStr) throws IOException {
        try {
            if (result instanceof FileMeta fileMeta && fileMeta.isTransferFile()) {
                RemoteCallerUtil.responseForFileMeta(fileMeta, response, log);
            } else {
                response.setContentType("application/json; charset=utf-8");
                response.getOutputStream().write(responseStr.getBytes(StandardCharsets.UTF_8));
            }

        } catch (IOException e) {
            response.sendError(500, e.getMessage());
        }
    }




    @NotNull
    private EngineMessageExecutionService getEngineMessageExecutionService() {
        EngineMessageExecutionService engineMessageExecutionService = InstanceFactory.instance(EngineMessageExecutionService.class, true);
        if (engineMessageExecutionService == null) {
            throw new BizException("commandExecutionService is null");
        }
        return engineMessageExecutionService;
    }

    public void executeServiceCaller(ServiceCaller serviceCaller, BiConsumer<Object, Throwable> resultCallback) {
        serviceCaller.setId(UUID.randomUUID().toString().replace("-", ""));
        serviceCaller.setReturnClass(Object.class.getName());
        Object[] args = serviceCaller.getArgs();
        DataMap context = null;
        if (args == null) {
            return;
        } else {
            Object[] newArgs = new Object[args.length + 1];
            System.arraycopy(args, 0, newArgs, 0, args.length);
            newArgs[args.length] = context;
            serviceCaller.setArgs(newArgs);
        }
        executeEngineMessage(serviceCaller, resultCallback);
    }
}

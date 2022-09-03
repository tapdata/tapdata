package io.tapdata.wsserver.channels.gateway;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.modules.api.net.data.*;
import io.tapdata.wsserver.channels.error.WSErrors;
import io.tapdata.wsserver.channels.gateway.modules.GatewayChannelModule;
import io.tapdata.pdk.core.utils.queue.ListErrorHandler;
import io.tapdata.pdk.core.utils.queue.ListHandler;

import java.util.List;
import java.util.Map;

public class UserActionHandler implements ListHandler<UserAction>, ListErrorHandler<UserAction> {
    public final String TAG = UserActionHandler.class.getSimpleName();


    private GatewayChannelModule gatewayChannelModule;

    private JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);

    public UserActionHandler() {

    }


    public void execute(UserAction userAction) throws CoreException {
            switch (userAction.getAction()) {
            case UserAction.ACTION_SESSION_CREATED:
                userAction.getHandler().touch();
                try {
                    userAction.getHandler().onSessionCreated();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    TapLogger.error(TAG, "onSessionCreated failed, {}, id {} on thread {}", throwable.getMessage(), userAction.getUserId(), Thread.currentThread());
                }
                break;
            case UserAction.ACTION_USER_CONNECTED:
                userAction.getHandler().touch();
                try {
                    userAction.getHandler().onChannelConnected();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    TapLogger.error(TAG, "onChannelConnected failed, {}, id {} on thread {}", throwable.getMessage(), userAction.getUserId(), Thread.currentThread());
                }
                break;
            case UserAction.ACTION_USER_DISCONNECTED:
                try {
                    userAction.getHandler().onChannelDisconnected();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    TapLogger.error(TAG, "onChannelDisconnected failed, {}, id {} on thread {}", throwable.getMessage(), userAction.getUserId(), Thread.currentThread());
                }
                break;
            case UserAction.ACTION_SESSION_DESTROYED:
                try {
                    userAction.getHandler().onSessionDestroyed();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    TapLogger.error(TAG, "onSessionDestroyed failed, {}, id {} on thread {}", throwable.getMessage(), userAction.getUserId(), Thread.currentThread());
                }
                break;
            case UserAction.ACTION_USER_CLOSURE:
                if (userAction.getClosure() != null) {
                    userAction.getHandler().touch();
                    try {
                        userAction.getClosure().run();
                    } catch (Throwable throwable1) {
                        throwable1.printStackTrace();
                        TapLogger.error(TAG, "userClosure call failed, closure {} error {}", userAction.getClosure(), throwable1.getMessage());
                    }
//                    userAction.closure.getMetaClass().invokeMethod(this, "doCall", userAction.closureArgs)
                }
                break;
            case UserAction.ACTION_USER_DATA:
                userAction.getHandler().touch();
                try {
                    ResultData resultData = userAction.getHandler().onDataReceived(userAction.getIncomingData());
                    if (resultData == null) {
                        resultData = new ResultData().code(ResultData.CODE_SUCCESS).forId(userAction.getIncomingData().getId());
                    }
                    if (resultData.getCode() == null)
                        resultData.setCode(ResultData.CODE_SUCCESS);
                    resultData.setForId(userAction.getIncomingData().getId());
                    boolean bool = gatewayChannelModule.sendResultData(userAction.getUserId(), resultData);
                    if (!bool) {
                        TapLogger.warn(TAG, "send result not successfully to userId {}, code {}", userAction.getUserId(), resultData.getCode());
                    }
                } catch (Throwable throwable) {
                    TapLogger.error(TAG, "onDataReceived contentType {} failed, {}", userAction.getIncomingData().getContentType(), throwable.getMessage());

                    Result errorResult = new Result().forId(userAction.getIncomingData().getId()).time(System.currentTimeMillis()).contentEncode(userAction.getIncomingData().getContentEncode());
                    if (throwable instanceof CoreException) {
                        errorResult.setCode(((CoreException) throwable).getCode());
                    } else {
                        errorResult.setCode(WSErrors.ERROR_UNKNOWN);
                    }
                    boolean bool = gatewayChannelModule.sendData(userAction.getUserId(), errorResult);
                    if (!bool) {
                        TapLogger.warn(TAG, "send errorResult not successfully to userId {}, code {} throwable {}", userAction.getUserId(), errorResult.getCode(), throwable.getMessage());
                    }
                }
                break;
            case UserAction.ACTION_USER_MESSAGE:
                userAction.getHandler().touch();
                try {
                    IncomingMessage incomingMessage = userAction.getIncomingMessage();
                    ResultData resultData = userAction.getHandler().onMessageReceived(incomingMessage);
                    if (resultData == null) {
                        resultData = new ResultData().code(ResultData.CODE_SUCCESS).forId(userAction.getIncomingData().getId());
                    }
                    if (resultData.getCode() == null)
                        resultData.setCode(ResultData.CODE_SUCCESS);
                    resultData.setForId(userAction.getIncomingData().getId());
                    boolean bool = gatewayChannelModule.sendResultData(userAction.getUserId(), resultData);
                    if (!bool) {
                        TapLogger.warn(TAG, "send result not successfully to userId $userAction.userId, code $resultData.code dataLength ${resultData.data}");
                    }
                } catch (Throwable throwable) {
//                    TapLogger.error(TAG, "onDataReceived contentType $userAction.incomingData.contentType failed, ${throwable.getMessage()}")
                    Result errorResult = new Result().forId(userAction.getIncomingMessage().getId()).time(System.currentTimeMillis()).contentEncode(userAction.getIncomingMessage().getContentEncode());
                    if (throwable instanceof CoreException) {
                        errorResult.setCode(((CoreException) throwable).getCode());
                    } else {
                        errorResult.setCode(WSErrors.ERROR_UNKNOWN);
                    }
                    boolean bool = gatewayChannelModule.sendData(userAction.getUserId(), errorResult);
                    if (!bool) {
                        TapLogger.warn(TAG, "send errorResult not successfully to userId {}, code {} throwable {}", userAction.getUserId(), errorResult.getCode(), throwable.getMessage());
                    }
                }
                break;
            case UserAction.ACTION_USER_OUTGOING_MESSAGE:
                try {
//                    Map jsonObject = JSON.parseObject(userAction.outgoingMessage.content)

                    userAction.getHandler().onOutgoingMessageReceived(userAction.getOutgoingMessage());
                } catch (Throwable throwable) {
                    TapLogger.error(TAG, "onOutgoingMessageReceived contentType {} failed, {}", userAction.getIncomingData().getContentType(), throwable.getMessage());
                }
                break;
            case UserAction.ACTION_USER_INVOCATION:
                userAction.getHandler().touch();
                try {
                    IncomingInvocation incomingInvocation = userAction.getIncomingInvocation();
                    ResultData resultData = userAction.getHandler().onInvocation(incomingInvocation);
                    if (resultData == null) {
                        resultData = new ResultData().code(ResultData.CODE_SUCCESS).forId(userAction.getIncomingInvocation().getId());
                    }
                    boolean bool = gatewayChannelModule.sendResultData(userAction.getUserId(), resultData);
                    if (!bool) {
                        TapLogger.warn(TAG, "send result not successfully to userId {}, code {} dataLength {}", userAction.getUserId(), resultData.getCode());
                    }
                } catch (Throwable throwable) {
                    TapLogger.error(TAG, "onInvocation userAction:{} {}", jsonParser.toJson(userAction.getIncomingInvocation()), throwable.getMessage());
                    Result errorResult = new Result().forId(userAction.getIncomingInvocation().getId()).time(System.currentTimeMillis()).contentEncode(userAction.getIncomingInvocation().getContentEncode());
                    if (throwable instanceof CoreException) {
                        errorResult.setCode(((CoreException) throwable).getCode());
                    } else {
                        errorResult.setCode(WSErrors.ERROR_UNKNOWN);
                    }
                    boolean bool = gatewayChannelModule.sendData(userAction.getUserId(), errorResult);
                    if (!bool) {
                        TapLogger.warn(TAG, "send errorResult not successfully to userId {}, code {} throwable {}", userAction.getUserId(), errorResult.getCode(), throwable.getMessage());
                    }
                }
                break;
            case UserAction.ACTION_USER_REQUEST:
                userAction.getHandler().touch();
                try {
                    IncomingRequest incomingRequest = userAction.getIncomingRequest();
                    ResultData resultData = userAction.getHandler().onRequest(incomingRequest);
                    if (resultData == null) {
                        resultData = new ResultData().code(ResultData.CODE_SUCCESS).forId(incomingRequest.getId());
                    }
                    boolean bool = gatewayChannelModule.sendResultData(userAction.getUserId(), resultData);
                    if (!bool) {
                        TapLogger.warn(TAG, "send result not successfully to userId {}, code {} dataLength {}", userAction.getUserId(), resultData.getCode());
                    }
                } catch (Throwable throwable) {
                    TapLogger.error(TAG, "onRequest {}", throwable.getMessage());
                    Result errorResult = new Result().forId(userAction.getIncomingRequest().getId()).time(System.currentTimeMillis());
                    if (throwable instanceof CoreException) {
                        errorResult.setCode(((CoreException) throwable).getCode());
                    } else {
                        errorResult.setCode(WSErrors.ERROR_UNKNOWN);
                    }
                    boolean bool = gatewayChannelModule.sendData(userAction.getUserId(), errorResult);
                    if (!bool) {
                        TapLogger.warn(TAG, "send errorResult not successfully to userId {}, code {} throwable {}", userAction.getUserId(), errorResult.getCode(), throwable.getMessage());
                    }
                }
                break;
            default:
                break;
        }
    }

    @Override
    public void execute(List<UserAction> list) throws Throwable {
        for(UserAction userAction : list) {
            execute(userAction);
        }
    }

    @Override
    public void error(List<UserAction> list, Throwable throwable) {
        TapLogger.error(TAG, "error occurred {} message {} roomAction {}", throwable, throwable.getMessage(), list);
    }
}

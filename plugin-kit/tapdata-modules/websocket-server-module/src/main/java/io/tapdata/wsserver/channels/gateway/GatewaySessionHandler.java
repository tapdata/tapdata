package io.tapdata.wsserver.channels.gateway;

import com.google.common.collect.Maps;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.data.*;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.wsserver.channels.error.WSErrors;
import io.tapdata.wsserver.channels.gateway.data.GatewayUserSession;
import io.tapdata.wsserver.channels.gateway.data.UserChannel;
import io.tapdata.wsserver.channels.gateway.modules.GatewayChannelModule;
import io.tapdata.wsserver.channels.websocket.utils.ValidateUtils;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class GatewaySessionHandler implements MemoryFetcher {
    private final String TAG = GatewaySessionHandler.class.getSimpleName();

    private long touch;

    private String token;
    /**
     * userId
     */
    private String id;
    private UserChannel userChannel;
    /**
     * 缓存该玩家事件更新的时间
     * 玩家登录后通知该缓存
     */
    private final Map<String, Long> cacheKeyToTimeMap = Maps.newConcurrentMap();
    @Bean
    private GatewayChannelModule gatewayChannelModule;

    public GatewaySessionHandler() {
        token = UUID.randomUUID().toString().replace("-", "");
    }

    public void touch() {
        touch = System.currentTimeMillis();
    }

    public abstract void onSessionCreated();
    /**
     * 通过验证，通道正式建连;同步
     * @return
     */
    public final Map<String, Long> channelConnected() { return cacheKeyToTimeMap; }

    /**
     * 通过验证，通道正式建连;异步单线程处理
     */
    public abstract void onChannelConnected();

    public abstract void onChannelDisconnected();

    public abstract void onSessionDestroyed();

    public abstract Result onDataReceived(IncomingData data);

    public abstract Result onMessageReceived(IncomingMessage message);

    public Result onRequest(IncomingRequest incomingRequest) {
        return new Result().code(Result.CODE_SUCCESS).forId(incomingRequest.getId());
    }

    public Result onInvocation(IncomingInvocation incomingInvocation) {
        ValidateUtils.checkAllNotNull(incomingInvocation, id);
        Pattern r = Pattern.compile(userChannel.getAuthorisedExpression());
        String matchStr = incomingInvocation.getService() + "_" + incomingInvocation.getClassName() + "_" + incomingInvocation.getMethodName();
        Matcher m = r.matcher(matchStr);
        if (!m.matches()) {
            throw new CoreException(WSErrors.ERROR_UNAUTHORISED_SERVICE_CALL, "Unauthorised calling to service $incomingInvocation authorisedExpression $userChannel.authorisedExpression");
        }
        Object[] args = null;
//        if (incomingInvocation.getArgs() != null) {
//            try {
//                JSONArray jsonArray = JSON.parseArray(incomingInvocation.getArgs());
//                args = jsonArray.toArray();
//            } catch (Throwable t) {
//                t.printStackTrace();
//                TapLogger.error(TAG, "Parse args string to json array failed, " + t.getMessage() + " incomingInvocation " + incomingInvocation + " id " + id);
//                throw t;
//            }
//        }
        GatewayUserSession gatewayUserSession = new GatewayUserSession().userId(this.id).ip(userChannel.getIp()).terminal(userChannel.getTerminal());
        if (args == null) {
            args = new Object[]{gatewayUserSession};
        } else {
            Object[] theArgs = new Object[args.length + 1];
            theArgs[0] = gatewayUserSession;
            if (theArgs.length > 1) {
                System.arraycopy(args, 0, theArgs, 1, args.length);
            }
            args = theArgs;
        }
//        TapLogger.info(TAG, "context " + context)
//        RPCCaller rpcCaller = context.getRPCCaller()
//
//        long startTime = System.currentTimeMillis();
//        Object returnObj = rpcCaller.call(incomingInvocation.service, incomingInvocation.className, incomingInvocation.methodName, Object.class, args)
//        long endTime = System.currentTimeMillis();
//        TapLogger.info(TAG, "invoke $incomingInvocation.className,method:$incomingInvocation.methodName args:${MessageUtils.toJSONString(args)} use time :${endTime - startTime}")
//
//
//        ResultData resultData = new ResultData(code: ResultData.CODE_SUCCESS, forId: id)
//        if (null != returnObj) {
//            resultData.data = JSON.toJSONString(returnObj, SerializerFeature.DisableCircularReferenceDetect)
//        }
//        return resultData
        return null;
    }

    /**
     * Verify successfully by default
     * Override this method for business logic and throw CoreException when token is illegal
     *
     * @param authorisedToken
     */
    public void verifyAuthorisedToken(String authorisedToken) {

    }


    void onOutgoingMessageReceived(OutgoingMessage outgoingMessage) {
        gatewayChannelModule.sendData(userChannel.getUserId(), outgoingMessage);
    }

    void onOutgoingDataReceived(OutgoingData outgoingData) {
        gatewayChannelModule.sendData(userChannel.getUserId(), outgoingData);
    }

    public boolean sendData(Data data) {
        return gatewayChannelModule.sendData(userChannel.getUserId(), data);
    }

    public boolean isChannelActive() {
        return gatewayChannelModule.isChannelActive(userChannel.getUserId());
    }

    public boolean sendData(String userId, String contentType, TapEntity data) {
        OutgoingData outgoingData = new OutgoingData().contentType(contentType).message(data).time(System.currentTimeMillis());
        return gatewayChannelModule.sendData(userId, outgoingData);
    }

    public Map<String, Long> getCacheKeyToTime() {
        return cacheKeyToTimeMap;
    }

    public UserChannel getUserChannel() {
        return userChannel;
    }

    public void setUserChannel(UserChannel userChannel) {
        this.userChannel = userChannel;
    }

    public String getId() {
        return id;
    }

    public long getTouch() {
        return touch;
    }

    public String getToken() {
        return token;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setToken(String token) {
        this.token = token;
    }
    public DataMap memory(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
                .kv("touch", new Date(touch))
                .kv("token", token)
                .kv("id", id)
                .kv("userChannel", userChannel);
    }
}

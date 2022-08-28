package io.tapdata.wsclient.modules.imclient.data;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

public abstract class IMMessage extends IMData{
    /**
     * Who send this message.
     */
    @JSONField(serialize = false)
    private String userId;
    /**
     * Which service is the sender from.
     */
    @JSONField(serialize = false)
    private String userService;

    @JSONField(serialize = false)
    private List<String> targetIds;

    @JSONField(serialize = false)
    private String targetService;

    public IMMessage() {
    }

    public static IMMessage buildSendingMessage(String contentType, List<String> targetIds, String targetService, Class<? extends IMMessage> messageClass) {
        IMMessage message = null;
        if(messageClass != null) {
            try {
                message = messageClass.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            if(message != null) {
                message.setContentType(contentType);
                message.setTargetIds(targetIds);
                message.setTargetService(targetService);
            }
        }
        if(message == null ||
                message.getContentType() == null ||
                message.getTargetIds() == null || message.getTargetIds().isEmpty() ||
                message.getTargetService() == null)
            throw new IllegalArgumentException("Build sending message illegal, need contentType, targetIds, targetService");
        return message;
    }

    public static IMMessage buildReceivingMessage() {
        return null;
    }

    public String getUserService() {
        return userService;
    }

    public void setUserService(String userService) {
        this.userService = userService;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public List<String> getTargetIds() {
        return targetIds;
    }

    public void setTargetIds(List<String> targetIds) {
        this.targetIds = targetIds;
    }

    public String getTargetService() {
        return targetService;
    }

    public void setTargetService(String targetService) {
        this.targetService = targetService;
    }
}

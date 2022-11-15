package com.tapdata.tm.commons.websocket.v1;

import com.tapdata.tm.commons.websocket.MessageInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/13 下午5:07
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MessageInfoV1 extends MessageInfo {
    public final static String VERSION = "v1";
    public final static char DELIMITER = ':';
    public final static String RETURN_TYPE = "return";
    private String type;
    private String body;
    private String beanName;
    private String methodName;

    public MessageInfoV1(){ }

    public static MessageInfoV1 parse(String originalMessage) {

        if (originalMessage.startsWith(VERSION)) {
            MessageInfoV1 messageInfo = new MessageInfoV1();
            String[] stringArray = new String[4];
            int length = originalMessage.length(),
                    start = 0,
                    partIndex = 0,
                    partLength = stringArray.length;
            for (int i = 0; i < length && partIndex < partLength; i++) {
                if (originalMessage.charAt(i) == DELIMITER) {
                    stringArray[partIndex] = originalMessage.substring(start, i);
                    partIndex++;
                    start = i + 1;
                }
                if (partIndex == partLength - 1) {
                    stringArray[partIndex] = originalMessage.substring(start, length);
                    break;
                }
            }
            if (partIndex == partLength - 1) {
                messageInfo.setVersion(stringArray[0]);
                messageInfo.setReqId(stringArray[1]);
                messageInfo.setType(stringArray[2]);
                messageInfo.setBody(stringArray[3]);

                if (messageInfo.getType().contains("/")) {
                    String[] arr = messageInfo.getType().split("/");
                    messageInfo.setBeanName(arr[0]);
                    messageInfo.setMethodName(arr[1]);
                }

                return messageInfo;
            }
        }
        return null;
    }

    @Override
    public String toTextMessage() {
        return getVersion() + DELIMITER +
                getReqId() + DELIMITER +
                getType() + DELIMITER +
                getBody();
    }
}

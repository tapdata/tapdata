package io.tapdata.wsclient.modules.imclient.impls.data;

import java.io.IOException;
import java.util.Set;

/**
 * IncomingMessage转换成为Message之后， IncomingMessage的id会是Message的clientId。 
 * 这个转化过程是在Gateway服务器进行的
 * 
 * @author aplombchen
 *
 */
public class IncomingMessage extends Data {
	
	public IncomingMessage() {
		super(HailPack.TYPE_IN_INCOMINGMESSAGE);
	}
	private String id;
	/**
	 * 该消息要发送的业务服务器
	 * singlechat/* 代表发送给单聊服务器的任何一台服务器。 
	 * singlechat/skdfjea 代表发送给单聊服务器的服务器名称为skdfjea的服务器。 
	 */
	private String server;

	/*
	这条消息来源于哪个Service
	 */
	private String service;

	/*
	消息发送给你用户的所属Service
	 */
	private String userService;
	
	private Set<String> userIds;
	
	private String contentType;
	private Integer contentEncode;
	private byte[] content;
	private Boolean notSaveOfflineMessage;
	
//	public Message toMessage(String userId) {
//		Message msg = new Message();
//		msg.setClientId(id);
//		msg.setId(script.memodb.ObjectId.get().toString());
//		msg.setReceiverIds(userIds);
//		msg.setServer(server);
//		msg.setService(service);
//		msg.setTime(System.currentTimeMillis());
//		msg.setType(contentType);
//		msg.setData(content);
//		msg.setEncode(contentEncode);
//		msg.setUserId(userId);
//		msg.setReceiverService(userService);
//		if(notSaveOfflineMessage == null)
//			notSaveOfflineMessage = false;
//		msg.setNotSaveOfflineMessage(notSaveOfflineMessage);
//		return msg;
//	}
	
	@Override
	public void resurrect() throws IOException {
		byte[] bytes = getData();
		Byte encode = getEncode();
		if(bytes != null) {
			if(encode != null) {
				switch(encode) {
				case ENCODE_PB:

					break;
					default:
						throw new IOException("Encoder type doesn't be found for resurrect");
				}
			}
		}
	}

	@Override
	public void persistent() throws IOException {
		Byte encode = getEncode();
		if(encode == null)
			encode = ENCODE_PB;//throw new CoreException(CoreErrorCodes.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent");
		switch(encode) {
		case ENCODE_PB:

			break;
			default:
				throw new IOException("Encoder type doesn't be found for persistent");
		}
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public Set<String> getUserIds() {
		return userIds;
	}

	public void setUserIds(Set<String> userIds) {
		this.userIds = userIds;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getContentEncode() {
		return contentEncode;
	}

	public void setContentEncode(Integer contentEncode) {
		this.contentEncode = contentEncode;
	}

	public String getUserService() {
		return userService;
	}

	public void setUserService(String userService) {
		this.userService = userService;
	}

	public Boolean getNotSaveOfflineMessage() {
		return notSaveOfflineMessage;
	}

	public void setNotSaveOfflineMessage(Boolean notSaveOfflineMessage) {
		this.notSaveOfflineMessage = notSaveOfflineMessage;
	}
}
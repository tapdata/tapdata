package io.tapdata.wsclient.modules.imclient.impls.data;

import java.io.IOException;

public class OutgoingMessage extends Data {
	
	public OutgoingMessage() {
		super(HailPack.TYPE_OUT_OUTGOINGMESSAGE);
	}
	private String id;
	private String userId;
	private String service;
	
	private Long time;
	
	private String contentType;
	private Integer contentEncode;
	private byte[] content;
	
	private Boolean needAck;
	
	////////////////////////only for keep Message in memory
//	private Message message;
	
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

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
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

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
//	public void fromMessage(Message message) {
//		this.message = message;
//		content = message.getData();
//		contentType = message.getType();
//		contentEncode = message.getEncode();
//		id = message.getId();
//		service = message.getService();
//		time = message.getTime();
//		userId = message.getUserId();
//		Boolean notSaveOfflineMessage = message.getNotSaveOfflineMessage();
//		if(notSaveOfflineMessage == null)
//			notSaveOfflineMessage = false;
//		needAck = !notSaveOfflineMessage;
//	}

	public Boolean getNeedAck() {
		return needAck;
	}

	public void setNeedAck(Boolean needAck) {
		this.needAck = needAck;
	}

//	public Message getMessage() {
//		return message;
//	}
//
//	public void setMessage(Message message) {
//		this.message = message;
//	}

	public Integer getContentEncode() {
		return contentEncode;
	}

	public void setContentEncode(Integer contentEncode) {
		this.contentEncode = contentEncode;
	}
}
package com.dobybros.tccore.modules.imclient.impls.data;


import java.io.IOException;

/**
 * IncomingData转换成为Message之后， IncomingData的id会是Message的clientId。
 * 这个转化过程是在Gateway服务器进行的
 * 
 * @author aplombchen
 *
 */
public class IncomingData extends Data {
	private String id;
	private String service;
	private String contentType;
	private Integer contentEncode;
	private byte[] content;

	public IncomingData() {
		super(HailPack.TYPE_IN_INCOMINGDATA);
	}

//	// 为向同一个serviceUser的不同terminal发消息而生
//	public Message toMessage(String userId) {
//		Message msg = new Message();
//		msg.setClientId(id);
//		msg.setId(script.memodb.ObjectId.get().toString());
//		Set<String> userIds = new HashSet<>();
//		userIds.add(userId);
//		msg.setReceiverIds(userIds);
//		msg.setService(service);
//		msg.setTime(System.currentTimeMillis());
//		msg.setType(contentType);
//		msg.setData(content);
//		msg.setEncode(contentEncode);
//		msg.setUserId(userId);
//		msg.setReceiverService(service);
//		msg.setNotSaveOfflineMessage(true);
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

}
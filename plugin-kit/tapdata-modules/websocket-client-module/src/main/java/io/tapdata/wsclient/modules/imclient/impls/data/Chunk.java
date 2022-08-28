package io.tapdata.wsclient.modules.imclient.impls.data;

import java.io.IOException;

public class Chunk extends Data {
	private String id;
	private Integer originalType;
	private byte[] content;
	private Integer offset;
	private Integer totalSize;
	private Integer chunkNum;

	public Chunk() {
		super(HailPack.TYPE_INOUT_CHUNK);
	}

	
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

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getOriginalType() {
		return originalType;
	}

	public void setOriginalType(Integer originalType) {
		this.originalType = originalType;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public Integer getOffset() {
		return offset;
	}

	public void setOffset(Integer offset) {
		this.offset = offset;
	}

	public Integer getTotalSize() {
		return totalSize;
	}

	public void setTotalSize(Integer totalSize) {
		this.totalSize = totalSize;
	}

	public Integer getChunkNum() {
		return chunkNum;
	}

	public void setChunkNum(Integer chunkNum) {
		this.chunkNum = chunkNum;
	}
}
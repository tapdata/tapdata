package io.tapdata.wsclient.modules.imclient.impls.data;

import io.tapdata.modules.api.net.data.Data;

import java.io.IOException;

public class Chunk extends Data {
	public static final byte TYPE = 61;
	private String id;
	private Integer originalType;
	private byte[] content;
	private Integer offset;
	private Integer totalSize;
	private Integer chunkNum;

	public Chunk() {
		super(TYPE);
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
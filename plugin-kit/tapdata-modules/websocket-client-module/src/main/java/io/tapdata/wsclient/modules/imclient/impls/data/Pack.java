package io.tapdata.wsclient.modules.imclient.impls.data;

import io.tapdata.wsclient.utils.LoggerEx;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class Pack {
	public static final byte TYPE_IN_SERVER =  2;
	public static final byte TYPE_IN_IDENTITY = 1;
	public static final byte TYPE_OUT_OUTGOINGMESSAGE = 10;
	public static final byte TYPE_OUT_OUTGOINGDATA = 11;
	public static final byte TYPE_IN_INCOMINGMESSAGE = 15;
	public static final byte TYPE_IN_INCOMINGDATA = 16;
	public static final byte TYPE_IN_ACKNOWLEDGE =  3;
	public static final byte TYPE_OUT_RESULT = 100;
	public static final byte TYPE_IN_PING = -3;
	public static final byte TYPE_INOUT_CHUNK = 20;
	private static final String TAG = Pack.class.getSimpleName();
	/**
	 * 封包结构的版本
	 */
	protected byte version;
	
	/**
	 * 类型
	 */
	protected byte type;
	
	/**
	 * 二进制内容
	 */
	protected byte[] content;
	/**
	 * content的字节长度
	 */
	protected int length;
	
	public Pack() {
	}
	
	public Pack(Data data) {
		setData(data);
	}
	
	public abstract void readHeadFromIoBuffer(DataInputStream buf) throws IOException;
	public abstract void persistent(DataOutputStream buf) throws IOException;
	
	public void setData(Data data) {
		if(data.getData() == null) {
			try {
				data.persistent();
			} catch (Throwable t) {
				t.printStackTrace();
				LoggerEx.error("Persistent data failed, " + " data " + data, t);
			}
		}
		if(data.getData() == null) {
			throw new NullPointerException("Data persistent failed, " + data);
		}
		type = data.getType();
		content = data.getData();
		length = content.length;
	}
	
	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public byte getVersion() {
		return version;
	}

	public void setVersion(byte version) {
		this.version = version;
	}
}

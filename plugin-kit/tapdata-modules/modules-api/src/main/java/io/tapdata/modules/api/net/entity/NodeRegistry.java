package io.tapdata.modules.api.net.entity;

public class NodeRegistry {
	private String ip;
	public NodeRegistry ip(String ip) {
		this.ip = ip;
		return this;
	}
	private Integer wsPort;
	public NodeRegistry wsPort(Integer wsPort) {
		this.wsPort = wsPort;
		return this;
	}
	private Integer httpPort;
	public NodeRegistry httpPort(Integer httpPort) {
		this.httpPort = httpPort;
		return this;
	}
	private String type;
	public NodeRegistry type(String type) {
		this.type = type;
		return this;
	}
	private Long time;
	public NodeRegistry time(Long time) {
		this.time = time;
		return this;
	}


//	@Override
//	public void from(InputStream inputStream) throws IOException {
//		DataInputStreamEx dis = dataInputStream(inputStream);
//		ip = dis.readUTF();
//		wsPort = dis.readInt();
//		httpPort = dis.readInt();
//		type = dis.readUTF();
//		time = dis.readLong();
//	}
//
//	@Override
//	public void to(OutputStream outputStream) throws IOException {
//		DataOutputStreamEx dos = dataOutputStream(outputStream);
//		dos.writeUTF(ip);
//		dos.writeInt(wsPort);
//		dos.writeInt(httpPort);
//		dos.writeUTF(type);
//		dos.writeLong(time);
//	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Integer getWsPort() {
		return wsPort;
	}

	public void setWsPort(Integer wsPort) {
		this.wsPort = wsPort;
	}

	public Integer getHttpPort() {
		return httpPort;
	}

	public void setHttpPort(Integer httpPort) {
		this.httpPort = httpPort;
	}

	public String id() {
		return ip + ":" + httpPort;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}
}

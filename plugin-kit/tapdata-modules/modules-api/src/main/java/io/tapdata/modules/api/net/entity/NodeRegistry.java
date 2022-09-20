package io.tapdata.modules.api.net.entity;

import io.tapdata.modules.api.utils.APIUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NodeRegistry {
	private List<String> ips;
	public NodeRegistry ip(String ip) {
		if(ips == null)
			ips = new ArrayList<>();
		ips.add(ip);
		return this;
	}
	public NodeRegistry ips(List<String> ips) {
		if(ips == null)
			return this;
		if(this.ips != null) {
			for(String newIp : ips) {
				if(!this.ips.contains(newIp)) {
					this.ips.add(newIp);
				}
			}
			return this;
		}
		this.ips = ips;
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

	@Override
	public String toString() {
		return "NodeRegistry ips " + ips + " httpPort " + httpPort + " wsPort " + wsPort + " type " + type + " time " + (time != null ? new Date(time) : null) + "; ";
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

	public List<String> getIps() {
		return ips;
	}

	public void setIps(List<String> ips) {
		this.ips = ips;
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

	private volatile String cachedId = null;
	public String id() {
		if(cachedId == null) {
			synchronized (this) {
				if(cachedId == null) {
					List<String> strings = new ArrayList<>(ips);
					strings.add(String.valueOf(httpPort));
					strings.add(String.valueOf(wsPort));
					strings.add(type);
					cachedId = APIUtils.idForList(strings);
				}
			}
		}
		return cachedId;
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

package io.tapdata.modules.api.net.entity;

import io.tapdata.modules.api.net.data.Data;

import java.util.Date;

public class NodeHealth implements Comparable<NodeHealth> {
	private String id;
	public NodeHealth id(String id) {
		this.id = id;
		return this;
	}
	private Long time;
	public NodeHealth time(Long time) {
		this.time = time;
		return this;
	}

	/**
	 * The smaller, the better
	 */
	private Integer health;
	public NodeHealth health(Integer health) {
		this.health = health;
		return this;
	}

	private Integer online;
	public NodeHealth online(Integer online) {
		this.online = online;
		return this;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public Integer getHealth() {
		return health;
	}

	public void setHealth(Integer health) {
		this.health = health;
	}

//	@Override
//	public void from(InputStream inputStream) throws IOException {
//		DataInputStreamEx dis = dataInputStream(inputStream);
//		id = dis.readUTF();
//		time = dis.readLong();
//		health = dis.readInt();
//	}
//
//	@Override
//	public void to(OutputStream outputStream) throws IOException {
//		DataOutputStreamEx dos = dataOutputStream(outputStream);
//		dos.writeUTF(id);
//		dos.writeLong(time);
//		dos.writeInt(health);
//	}
	@Override
	public int compareTo(NodeHealth sessionClassHolder) {
//        if(order == interceptorClassHolder.order)
//            return 0;
		return health > sessionClassHolder.health ? -1 : 1;
	}

	@Override
	public String toString() {
		return "NodeHealth id " + id + " health " + health + " online " + online + " time " + (time != null ? new Date(time) : null) + "; ";
	}

	public Integer getOnline() {
		return online;
	}

	public void setOnline(Integer online) {
		this.online = online;
	}

	public void clone(NodeHealth nodeHealth) {
		this.health = nodeHealth.health;
		this.time = nodeHealth.time;
		this.online = nodeHealth.online;
		this.id = nodeHealth.id;
	}
}

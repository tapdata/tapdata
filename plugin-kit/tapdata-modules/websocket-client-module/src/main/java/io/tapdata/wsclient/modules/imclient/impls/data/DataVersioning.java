package io.tapdata.wsclient.modules.imclient.impls.data;

import io.tapdata.wsclient.utils.LoggerEx;

import java.io.IOException;

public class DataVersioning{
	private static final String TAG = DataVersioning.class.getSimpleName();

	public static final byte version = 1;
	public static final short encodeVersion = 1;
	public static final byte encode = HailPack.ENCODE_PB;

	public static Pack getDataPack(Data data) {
		data.setEncodeVersion(encodeVersion);
		data.setEncode(encode);
		HailPack hailPack = new HailPack(data);
		hailPack.setVersion(version);
		return hailPack;
	}

	public static Data get(Pack pack) {
		HailPack hailPack = (HailPack) pack;
		Data data = get(hailPack.getEncodeVersion(), hailPack.getType());
		if(data != null) {
			data.setData(pack.getContent());
			data.setEncode(hailPack.getEncode());
			try {
				data.resurrect();
				return data;
			} catch (IOException e) {
				e.printStackTrace();
				LoggerEx.error("Resurrect data failed, " + e.getMessage() + " for data " + data, e);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static Data get(short encodeVersion, int type) {
		//new Acknowledge() 比Acknowledge.class.newInstance快1.5倍
		Data data = null;
		switch(type) {
		case HailPack.TYPE_IN_ACKNOWLEDGE:
			data = new Acknowledge();
			break;
		case HailPack.TYPE_IN_IDENTITY:
			data = new Identity();
			break;
		case HailPack.TYPE_IN_INCOMINGMESSAGE:
			data = new IncomingMessage();
			break;
		case HailPack.TYPE_OUT_OUTGOINGMESSAGE:
			data = new OutgoingMessage();
			break;
		case HailPack.TYPE_OUT_RESULT:
			data = new Result();
			break;
		case HailPack.TYPE_IN_PING:
			data = new Ping();
			break;
		case HailPack.TYPE_IN_INCOMINGDATA:
			data = new IncomingData();
			break;
		case HailPack.TYPE_OUT_OUTGOINGDATA:
			data = new OutgoingData();
			break;
		}
		if(data != null)
			data.setEncodeVersion(encodeVersion);
		return data;
	}

	public static void main(String[] args) {
		int count = 1000000;
		long time = System.currentTimeMillis();
		for(int i = 0; i < count; i++) {
			DataVersioning.get((short) 1, HailPack.TYPE_IN_ACKNOWLEDGE);
//			new Acknowledge(); //40ms
//			try {
//				Acknowledge.class.newInstance(); //100ms
//			} catch (InstantiationException | IllegalAccessException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		System.out.println(System.currentTimeMillis() - time);
	}
	
}

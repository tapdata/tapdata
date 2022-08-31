package io.tapdata.wsclient.modules.imclient.impls.data;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.*;

import java.io.IOException;

public class DataVersioning{
	private static final String TAG = DataVersioning.class.getSimpleName();


	@SuppressWarnings("unchecked")
	public static Data get(short encodeVersion, int type) {
		//new Acknowledge() 比Acknowledge.class.newInstance快1.5倍
		Data data = null;
		switch(type) {
//		case HailPack.TYPE_IN_ACKNOWLEDGE:
//			data = new Acknowledge();
//			break;
		case Identity.TYPE:
			data = new Identity();
			break;
		case IncomingMessage.TYPE:
			data = new IncomingMessage();
			break;
		case OutgoingMessage.TYPE:
			data = new OutgoingMessage();
			break;
		case Result.TYPE:
			data = new Result();
			break;
		case Ping.TYPE:
			data = new Ping();
			break;
		case IncomingData.TYPE:
			data = new IncomingData();
			break;
		case OutgoingData.TYPE:
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
//			DataVersioning.get((short) 1, HailPack.TYPE_IN_ACKNOWLEDGE);
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

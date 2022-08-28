package io.tapdata.wsclient.modules.imclient.impls.data;


public class PackVersioning {

	public static int getHeadLength(int version) {
		switch(version) {
		case 1:
			return HailPack.getPackHeadLength();
		}
		return -1;
	}

	public static Pack get(Byte version, Byte encode, Short encodeVersion) {
		HailPack hailPack = new HailPack();
		hailPack.setVersion(version);
		hailPack.setEncode(encode);
		hailPack.setEncodeVersion(encodeVersion);
		return hailPack;
	}

	public static Pack get(Byte version, Data data) {
		HailPack hailPack = new HailPack();
		hailPack.setVersion(version);
		hailPack.setData(data);
		return hailPack;
	}

}

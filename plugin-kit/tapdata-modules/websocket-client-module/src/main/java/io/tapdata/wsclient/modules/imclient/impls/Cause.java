package io.tapdata.wsclient.modules.imclient.impls;

import java.util.HashMap;
import java.util.Map;

public class Cause {
	public static final int CAUSE_OKAY = 0;
	public static final int CAUSE_CONNECT_FAILED = 1;
	public static final int CAUSE_READWRITE_FAILED = 2;
	public static final int CAUSE_NO_USER = 3;
	public static final int CAUSE_FINISHED = 4;
	public static final int CAUSE_LOGIC_FAILED = 5;
//	public static final int CAUSE_ERROR_NEED2CLOSEPUSHCHANCEL = 6;
//	public static final int CAUSE_SIDISNULL_NEED2AUTOLOGIN = 7;
	public static final int CAUSE_NEED2AUTOLOGIN = 8;
	public static final int CAUSE_SERVER_NO_USER = 9;
	public static final int CAUSE_ERROR_PSWCHANGED = 10;
	public static final int CAUSE_ERROR_LOGINOTERDEVICE = 11;
	public static final int CAUSE_ERROR_UNKNOWN = 12;
	
	public static Map<Integer, Integer> causeMap = new HashMap<>();
	static {
		causeMap.put(4001, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4002, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4003, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4004, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4005, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4006, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4007, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4008, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4009, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(4010, CAUSE_NEED2AUTOLOGIN);
		
		causeMap.put(4010, CAUSE_NEED2AUTOLOGIN);
		causeMap.put(1016, CAUSE_ERROR_LOGINOTERDEVICE);
		causeMap.put(57, CAUSE_ERROR_PSWCHANGED);
	}
	
	public static Integer getCause(Integer serverCode) {
		Integer value = causeMap.get(serverCode);
		if(value == null)
			return CAUSE_ERROR_UNKNOWN;
		return value;
	}
}

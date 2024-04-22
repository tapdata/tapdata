package com.tapdata.constant;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2024-04-19 12:40
 **/
public class SerializableObject implements Serializable {
	private static final long serialVersionUID = 7846142218350734402L;
	private int _int;
	private String _str;

	public SerializableObject(int _int, String _str) {
		this._int = _int;
		this._str = _str;
	}

	public int get_int() {
		return _int;
	}

	public void set_int(int _int) {
		this._int = _int;
	}

	public String get_str() {
		return _str;
	}

	public void set_str(String _str) {
		this._str = _str;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SerializableObject) {
			return _int == ((SerializableObject) obj)._int && _str.equals(((SerializableObject) obj)._str);
		} else {
			return false;
		}
	}
}

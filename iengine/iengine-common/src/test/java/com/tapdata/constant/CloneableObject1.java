package com.tapdata.constant;

/**
 * @author samuel
 * @Description
 * @create 2024-04-19 12:41
 **/
public class CloneableObject1 implements Cloneable {
	private int _int;
	private String _str;

	public CloneableObject1(int _int, String _str) {
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
		if (obj instanceof CloneableObject1) {
			return _int == ((CloneableObject1) obj)._int && _str.equals(((CloneableObject1) obj)._str);
		} else {
			return false;
		}
	}
}

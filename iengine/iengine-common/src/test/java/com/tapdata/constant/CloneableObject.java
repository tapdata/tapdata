package com.tapdata.constant;

/**
 * @author samuel
 * @Description
 * @create 2024-04-19 12:41
 **/
public class CloneableObject implements Cloneable {
	private int _int;
	private String _str;

	public CloneableObject(int _int, String _str) {
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
	protected Object clone() throws CloneNotSupportedException {
		Object clone = super.clone();
		if (clone instanceof CloneableObject) {
			((CloneableObject) clone).set_str(_str);
			((CloneableObject) clone).set_int(_int);
		}
		return clone;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CloneableObject) {
			return _int == ((CloneableObject) obj)._int && _str.equals(((CloneableObject) obj)._str);
		} else {
			return false;
		}
	}
}

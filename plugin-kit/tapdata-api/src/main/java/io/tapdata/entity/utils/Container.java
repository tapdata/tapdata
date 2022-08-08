package io.tapdata.entity.utils;

public class Container <T, P> {
	private T t;
	private P p;
	public Container(T t, P p) {
		this.t = t;
		this.p = p;
	}

	public T getT() {
		return t;
	}

	public void setT(T t) {
		this.t = t;
	}

	public P getP() {
		return p;
	}

	public void setP(P p) {
		this.p = p;
	}
}

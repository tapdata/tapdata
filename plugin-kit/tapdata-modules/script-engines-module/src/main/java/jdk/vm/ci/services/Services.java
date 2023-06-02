package jdk.vm.ci.services;

import com.oracle.truffle.polyglot.PolyglotImpl;
import org.graalvm.polyglot.impl.AbstractPolyglotImpl;

import java.util.Collections;
import java.util.ServiceLoader;

public class Services {

	public static Iterable<?> load(Class<?> clazz) {
		if (clazz == AbstractPolyglotImpl.class) {
			return Collections.singleton(new PolyglotImpl());
		} else {
			return ServiceLoader.load(clazz);
		}
	}
}

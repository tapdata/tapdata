package com.tapdata.entity.values;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory to wrap a database value into Tap Value Container.
 *
 * @author Dexter
 */
public class TapValueFactory {
	static Map<Class<? extends AbstractTapValue<?>>, Map<Class<?>, Constructor<?>>> cachedConstructors = new HashMap<>();

	/**
	 * Get the supported constructors from the Tap Value Container.
	 *
	 * @param tapValueClass: Class of the Tap Value Container.
	 * @return Supported constructors for the Tap Value Container.
	 */
	private static Map<Class<?>, Constructor<?>> getConstructorsFromClass(Class<? extends AbstractTapValue<?>> tapValueClass) {
		Map<Class<?>, Constructor<?>> constructors = new HashMap<>();
		for (Constructor<?> constructor : tapValueClass.getConstructors()) {
			Class<?>[] args = constructor.getParameterTypes();
			if (args.length > 0) {
				constructors.put(args[0], constructor);
			}
		}
		cachedConstructors.putIfAbsent(tapValueClass, constructors);

		return constructors;
	}

	/**
	 * Wrap a database value into Tap Value Container.
	 *
	 * @param tapValueClass Class of the Tap Value Container.
	 * @param value         The database value which is waiting to be wrapped into the container.
	 * @return Tap Value Container.
	 * @throws Exception
	 */
	public static AbstractTapValue<?> newTapValueContainer(Class<? extends AbstractTapValue<?>> tapValueClass, Object value)
			throws Exception {
		if (!cachedConstructors.containsKey(tapValueClass)) {
			// get the constructors of the Container
			cachedConstructors.putIfAbsent(tapValueClass, getConstructorsFromClass(tapValueClass));
		}

		Class<?> valueType = value.getClass();

		Constructor<?> constructor = cachedConstructors.get(tapValueClass).getOrDefault(valueType, null);
		// try to get the super class and see if it matches
		if (constructor == null) {
			Class<?> valueTypeSuper = value.getClass().getSuperclass();
			if (valueTypeSuper != Object.class && cachedConstructors.get(tapValueClass).containsKey(valueTypeSuper)) {
				constructor = cachedConstructors.get(tapValueClass).get(valueTypeSuper);
			}
		}
		// try to get the interfaces and see if it matches
		if (constructor == null) {
			for (Class<?> valueTypeInterface : value.getClass().getInterfaces()) {
				if (cachedConstructors.get(tapValueClass).containsKey(valueTypeInterface)) {
					constructor = cachedConstructors.get(tapValueClass).get(valueTypeInterface);
					break;
				}
			}
		}

		if (constructor == null) {
			throw new Exception(String.format("un-support type %s for container %s, the original-value is: %s", valueType.getName(), tapValueClass.getName(), value));
		}

		return (AbstractTapValue<?>) constructor.newInstance(value);
	}

}

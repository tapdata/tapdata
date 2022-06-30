package com.tapdata.constant;

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListUtil {


	public static <T extends Cloneable> void cloneableCloneList(List<T> oldList, List<T> newList)
			throws NoSuchMethodException,
			InvocationTargetException,
			IllegalAccessException {

		// Boundary conditions checked
		if (oldList == null
				|| oldList.size() == 0) {
			return;
		}

		Method method
				= oldList.get(0)
				.getClass()
				.getDeclaredMethod("clone");

		for (T item : oldList) {
			newList.add((T) method.invoke(item));
		}
	}

	public static void serialCloneList(List list, List newList) throws IllegalAccessException, InstantiationException {
		if (list == null) {
			return;
		}

		for (Object o : list) {
			if (o instanceof List) {
				List newObject = (List) o.getClass().newInstance();
				serialCloneList((List) o, newObject);
				newList.add(newObject);
			} else if (o instanceof Map) {
				Map newObject = (Map) o.getClass().newInstance();
				MapUtil.deepCloneMap((Map<String, Object>) o, newObject);

				newList.add(newObject);
			} else if (o instanceof Serializable) {
				Serializable serl = (Serializable) o;
				Serializable clone = SerializationUtils.clone(serl);
				newList.add(clone);
			} else {
				newList.add(o);
			}
		}
	}

	public static void copyList(List list, List newList) {
		if (list == null) {
			return;
		}

		for (Object o : list) {
			if (o instanceof ScriptObjectMirror &&
					((ScriptObjectMirror) o).isArray()
			) {
				List listObj = ((ScriptObjectMirror) o).to(List.class);
				List newObject = new ArrayList();
				copyList(listObj, newObject);
				newList.add(newObject);
			} else if (o instanceof List) {
				List newObject = new ArrayList();
				copyList((List) o, newObject);
				newList.add(newObject);
			} else if (o instanceof Map) {
				Map newObject = new HashMap();
				MapUtil.copyToNewMap((Map<String, Object>) o, newObject);

				newList.add(newObject);
			} else {
				newList.add(o);
			}
		}

	}
}

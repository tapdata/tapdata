package io.tapdata.modules.api.storage;

import java.util.Iterator;

/**
 * @author aplomb
 */
public interface TapSequenceStorage extends TapStorage {
	void add(Object data);
	Iterator<Object> iterator();

	void clear();
	void destroy();
}

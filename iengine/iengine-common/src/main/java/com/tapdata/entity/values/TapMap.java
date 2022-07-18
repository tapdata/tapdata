package com.tapdata.entity.values;

import java.util.Map;

/**
 * @author Dexter
 */
public class TapMap extends AbstractTapValue<Map<?, ?>> {

	// Constructors

	public TapMap() {
	}

	/**
	 * Accept a {@link Map} value into TapMap.
	 */
	public TapMap(Map<?, ?> origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin);
	}

	// Getters: Getting desired value from this

	@Override
	public String getString(AbstractTapValue<?> container) throws Exception {
		if (container instanceof TapMap) {
			return container.getOrigin().toString();
		}
		return container.getString(container);
	}

}

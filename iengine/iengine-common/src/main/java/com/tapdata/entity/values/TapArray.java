package com.tapdata.entity.values;

import java.util.Collection;

/**
 * @author Dexter
 */
public class TapArray extends AbstractTapValue<Collection<?>> {

	// Constructors

	public TapArray() {
	}

	/**
	 * Accept a {@link Collection} value into TapArray.
	 */
	public TapArray(Collection<?> origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin);
	}

	// Getters: Getting desired value from this

	@Override
	public String getString(AbstractTapValue<?> container) throws Exception {
		if (container instanceof TapArray) {
			return container.getOrigin().toString();
		}
		return container.getString(container);
	}

}

package io.tapdata.pdk.core.api.impl.serialize;

import io.tapdata.entity.utils.ObjectSerializable;

import java.io.DataOutputStream;

/**
 * @author aplomb
 */
public interface SerializeCodec {
	void serialize(Object obj, DataOutputStream dos, ObjectSerializable.FromObjectOptions fromObjectOptions);

}

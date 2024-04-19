package com.tapdata.entity.dataflow.batch;

import com.tapdata.constant.MapUtil;
import com.tapdata.exception.CloneException;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BatchOffset implements Serializable, Cloneable {
    private static final long serialVersionUID = 5599838762323297718L;

    public BatchOffset() {
    }

    public BatchOffset(Object offset, String status) {
        this.offset = offset;
        this.status = status;
    }

    /**
     * table offset
     * */
    Object offset;
    /**
     * table batch read status: over | running
     * */
    String status;

    public Object getOffset() {
        return offset;
    }

    public void setOffset(Object offset) {
        this.offset = offset;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

	@Override
	public Object clone() throws CloneNotSupportedException {
		Object clone = super.clone();
		if (clone instanceof BatchOffset) {
			BatchOffset batchOffset = (BatchOffset) super.clone();
			batchOffset.setStatus(status);
			if (offset instanceof Map) {
				Map<String, Object> newOffset = new HashMap<>();
				try {
					MapUtil.deepCloneMap((Map) offset, newOffset);
					batchOffset.setOffset(newOffset);
				} catch (IllegalAccessException | InstantiationException e) {
					throw new CloneException(e);
				}
			} else if (offset instanceof Serializable) {
				batchOffset.setOffset(SerializationUtils.clone((Serializable) offset));
			} else {
				byte[] bytes = InstanceFactory.instance(ObjectSerializable.class).fromObject(offset);
				batchOffset.setOffset(InstanceFactory.instance(ObjectSerializable.class).toObject(bytes));
			}
		}
		return clone;
	}

}

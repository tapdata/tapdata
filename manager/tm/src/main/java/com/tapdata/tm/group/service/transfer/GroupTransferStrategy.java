package com.tapdata.tm.group.service.transfer;

import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface GroupTransferStrategy {
    GroupTransferType getType();

    void exportGroups(GroupExportRequest request);

	default ObjectId importGroups(GroupImportRequest request) throws IOException {
		throw new UnsupportedOperationException();
	}

    default Map<String, List<TaskUpAndLoadDto>> parseImportPayloads(Object resource) throws IOException {
        throw new UnsupportedOperationException();
    }
}

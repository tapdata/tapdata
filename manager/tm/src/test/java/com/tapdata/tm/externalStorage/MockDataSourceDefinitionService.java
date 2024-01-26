package com.tapdata.tm.externalStorage;

import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import lombok.NonNull;

public class MockDataSourceDefinitionService extends DataSourceDefinitionService {

    private boolean flag = false;

    public MockDataSourceDefinitionService(@NonNull DataSourceDefinitionRepository repository) {
        super(repository);
    }

    @Override
    public DataSourceDefinitionDto findByPdkHash(String pdkHash, Integer pdkBuildNumber, UserDetail user, String... field) {
        this.flag = true;
        return new DataSourceDefinitionDto();
    }

    public boolean isFlag() {
        return flag;
    }
}

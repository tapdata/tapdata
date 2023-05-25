package com.tapdata.tm.lineage.analyzer;

import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.modules.repository.ModulesRepository;
import com.tapdata.tm.task.repository.TaskRepository;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author samuel
 * @Description
 * @create 2023-05-22 15:51
 **/
@Service
@Setter(onMethod_ = {@Autowired})
public abstract class BaseAnalyzer implements AnalyzerService {
	protected TaskRepository taskRepository;
	protected DataSourceRepository dataSourceRepository;
	protected ModulesRepository modulesRepository;
	protected MetadataInstancesRepository metadataInstancesRepository;
}

package com.tapdata.tm.lineage.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.lineage.analyzer.AnalyzerService;
import com.tapdata.tm.lineage.analyzer.entity.LineageNode;
import com.tapdata.tm.lineage.dto.LineageDto;
import com.tapdata.tm.lineage.dto.TableLineageDto;
import com.tapdata.tm.lineage.entity.LineageEntity;
import com.tapdata.tm.lineage.repository.LineageRepository;
import com.tapdata.tm.lineage.vo.TableLineageRequestVo;
import io.github.openlg.graphlib.Graph;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2023/05/19
 * @Description:
 */
@Service
@Slf4j
public class LineageService extends BaseService<LineageDto, LineageEntity, ObjectId, LineageRepository> {
	private final AnalyzerService analyzerService;

	public LineageService(@NonNull LineageRepository repository, @Qualifier("tableAnalyzerV1") AnalyzerService analyzerService) {
		super(repository, LineageDto.class, LineageEntity.class);
		this.analyzerService = analyzerService;
	}

	protected void beforeSave(LineageDto lineage, UserDetail user) {

	}

	public TableLineageDto tableLineage(TableLineageRequestVo tableLineageRequestVo) {
		if (tableLineageRequestVo.isEmpty()) {
			throw new IllegalArgumentException("connectionId, table can not be empty: " + tableLineageRequestVo);
		}
		try {
			Graph<Node, Edge> graph = analyzerService.analyzeTable(
					tableLineageRequestVo.getConnectionId(),
					tableLineageRequestVo.getTable(),
					tableLineageRequestVo.getLineageType()
			);
			if (null == graph) {
				graph = new Graph<>();
			}
			DAG dag = new DAG(graph);
			return new TableLineageDto(dag.toDag());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
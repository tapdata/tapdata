package io.tapdata.mongodb.net;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.service.NodeRegistryService;
import io.tapdata.mongodb.entity.IdEntity;
import io.tapdata.mongodb.entity.NodeRegistryEntity;
import io.tapdata.mongodb.entity.ToDocument;
import io.tapdata.mongodb.net.dao.IdEntityDAO;
import io.tapdata.mongodb.net.dao.NodeRegistryDAO;
import org.bson.Document;

@Implementation(NodeRegistryService.class)
public class NodeRegistryServiceImpl implements NodeRegistryService {
	@Bean
	private NodeRegistryDAO nodeRegistryDAO;
	@Override
	public void save(NodeRegistry nodeRegistry) {
		nodeRegistryDAO.insertOne(new NodeRegistryEntity(nodeRegistry.id(), nodeRegistry));
	}
	@Override
	public void delete(String id) {
		nodeRegistryDAO.delete(new Document(ToDocument.FIELD_ID, id));
	}
	@Override
	public NodeRegistry get(String id) {
		NodeRegistryEntity nodeRegistry = nodeRegistryDAO.findOne(new Document(ToDocument.FIELD_ID, id));
		if(nodeRegistry != null) {
			return nodeRegistry.getNode();
		}
		return null;
	}
}

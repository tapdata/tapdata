package io.tapdata.mongodb.net;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.service.node.NodeRegistryService;
import io.tapdata.mongodb.entity.NodeRegistryEntity;
import io.tapdata.mongodb.net.dao.NodeRegistryDAO;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static io.tapdata.mongodb.entity.ToDocument.FIELD_ID;

@Implementation(NodeRegistryService.class)
public class NodeRegistryServiceImpl implements NodeRegistryService {
	@Bean
	private NodeRegistryDAO nodeRegistryDAO;
	@Override
	public void save(NodeRegistry nodeRegistry) {
		nodeRegistryDAO.insertOne(new NodeRegistryEntity(nodeRegistry.id(), nodeRegistry));
	}

	@Override
	public boolean delete(String id) {
		return delete(id, null);
	}
	@Override
	public boolean delete(String id, Long time) {
		return nodeRegistryDAO.delete(id, time);
	}
	@Override
	public NodeRegistry get(String id) {
		NodeRegistryEntity nodeRegistry = nodeRegistryDAO.findOne(new Document(FIELD_ID, id));
		if(nodeRegistry != null) {
			return nodeRegistry.getNode();
		}
		return null;
	}

	@Override
	public List<String> getNodeIds() {
		List<NodeRegistryEntity> nodeRegistries = nodeRegistryDAO.find(new Document(), FIELD_ID);
		List<String> list = new ArrayList<>();
		if(nodeRegistries != null) {
			for(NodeRegistryEntity nodeRegistryEntity : nodeRegistries) {
				list.add(nodeRegistryEntity.getId());
			}
		}
		return list;
	}

	@Override
	public List<NodeRegistry> getNodes() {
		List<NodeRegistryEntity> nodeRegistries = nodeRegistryDAO.find(new Document());
		List<NodeRegistry> list = new ArrayList<>();
		if(nodeRegistries != null) {
			for(NodeRegistryEntity nodeRegistryEntity : nodeRegistries) {
				list.add(nodeRegistryEntity.getNode());
			}
		}
		return list;
	}
}

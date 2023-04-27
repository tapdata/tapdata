package com.tapdata.constant;

import com.tapdata.entity.dataflow.Stage;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Graph<T> {

	private Map<T, List<T>> paths = new HashMap<>();

	public Graph() {

	}

	private void initAdjList(List<T> vertices) {
		paths = new HashMap<>(vertices.size());

		for (T vertex : vertices) {
			paths.put(vertex, new ArrayList<>());
		}
	}

	public void addEdge(T u, T v) {
		if (!paths.containsKey(u)) {
			paths.put(u, new ArrayList<>());
		}
		List<T> childNodes = paths.get(u);

		if (!childNodes.contains(v)) {
			childNodes.add(v);
		}
	}

	public List<List<T>> printAllPaths(T s, T d) {
		Set<T> isVisited = new HashSet<>();
		ArrayList<T> pathList = new ArrayList<>();
		List<List<T>> allPathList = new ArrayList<>();

		pathList.add(s);

		printAllPathsUtil(s, d, isVisited, pathList, allPathList);

		return allPathList;
	}

	public Map<T, List<T>> getPaths() {
		return paths;
	}

	private void printAllPathsUtil(T u, T d,
								   Set<T> isVisited,
								   List<T> localPathList, List<List<T>> allPathList) {

		isVisited.add(u);

		if (u.equals(d)) {
			if (CollectionUtils.isNotEmpty(localPathList)) {
				List<T> newLocalPathList = new ArrayList<>();
				newLocalPathList.addAll(localPathList);
				allPathList.add(newLocalPathList);
			}

			isVisited.remove(u);
			return;
		}

		if (paths.containsKey(u)) {
			for (T i : paths.get(u)) {
				if (!isVisited.contains(i)) {

					localPathList.add(i);
					printAllPathsUtil(i, d, isVisited, localPathList, allPathList);

					localPathList.remove(i);
				}
			}
		}

		isVisited.remove(u);
	}

	public static void main(String[] args) {
		Map<String, Stage> stageById = new HashMap<>();
		List<Stage> stages = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			Stage stage = new Stage();
			stage.setId(String.valueOf(i));
			stages.add(stage);

			stageById.put(stage.getId(), stage);
		}

		stages.get(0).getOutputLanes().add("5");
		stages.get(0).getOutputLanes().add("9");
		stages.get(0).getOutputLanes().add("3");
		stages.get(0).getOutputLanes().add("2");
		stages.get(2).getOutputLanes().add("1");
		stages.get(2).getOutputLanes().add("4");
		stages.get(0).getOutputLanes().add("2");
		stages.get(9).getOutputLanes().add("8");
		stages.get(7).getOutputLanes().add("8");
		stages.get(6).getOutputLanes().add("8");
		stages.get(1).getOutputLanes().add("7");

		Graph g = new Graph();
		for (int i = 0; i < stages.size(); i++) {
			Stage stage = stages.get(i);
			List<String> outputLanes = stage.getOutputLanes();
			for (String outputLane : outputLanes) {
				g.addEdge(stage, stageById.get(outputLane));
			}
		}

		// arbitrary source
		int s = 0;

		// arbitrary destination
		int d = 8;

		for (Stage stage : stages) {
			for (Stage stage1 : stages) {
				if (stage.equals(stage1)) {
					continue;
				}
				System.out.println(stage + " -> " + stage1 + ":");
				List list = g.printAllPaths(stage, stage1);
				System.out.println(list);
			}
		}

	}
}

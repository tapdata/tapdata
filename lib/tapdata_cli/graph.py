class Node:
    """describe dag node"""
    def __init__(self, node_id: str, name: str, config: dict={}):
        self.name = name
        self.node_id = node_id
        self.config = config
        self.connectedTo = {}

    def addNeighbor(self, node):
        self.connectedTo.update({node.node_id: node})

    def __str__(self):
        if self.connectedTo:
            after_text = ' --> [' + ", ".join([str(nbr) for nbr in self.connectedTo.values()]) + "]"
        else:
            after_text = ""
        return f"{self.name}-({self.node_id[-6:]})" + after_text

    def getConnections(self):
        return self.connectedTo.keys()

    def getId(self):
        return self.node_id

    def getNbrNode(self, node_id):
        node = self.connectedTo.get(node_id)
        if node is not None:
            return node
        else:
            raise KeyError("No such nbr node exist!")


class Graph(object):
    """
    describe the relationship of node
    """
    def __init__(self):
        self.vertexList = {}
        self.vertexNum = 0
        self.child_node = {}
        self.head_node = {}

    def addVertex(self, *node: Node):
        for n in node:
            self.vertexList.update({n.node_id: n})
            self.vertexNum += 1

    def getVertex(self, node_id):
        node = self.vertexList.get(node_id)
        return node

    def __contains__(self, query):
        return query in self.vertexList.values() or query in self.vertexList.keys()

    def _addEdge(self, f, t):
        self.child_node.update({t.node_id: t})
        if self.head_node.get(t.node_id):
            del(self.head_node[t.node_id])
        if not self.child_node.get(f.node_id):
            self.head_node.update({f.node_id: f})
        f.addNeighbor(t)

    def addEdge(self, f, t):
        f, t = self.getVertex(f.node_id), self.getVertex(t.node_id)
        if not f:
            self.addVertex(f)
        if not t:
            self.addVertex(t)
        self._addEdge(f, t)
    
    def addEdgeById(self, f_id, t_id):
        if not self.getVertex(f_id):
            raise KeyError(f_id)
        if not self.getVertex(t_id):
            raise KeyError(t_id)
        f, t = self.getVertex(f_id), self.getVertex(t_id)
        self._addEdge(f, t)

    def getVertices(self):
        return self.vertexList.keys()

    def __iter__(self):
        return iter(self.vertexList.values())
    
    def to_relation(self):
        relations = []
        for n in self.head_node.values():
            relations.append(str(n))
        return relations

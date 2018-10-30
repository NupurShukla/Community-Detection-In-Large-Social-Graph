from pyspark import SparkContext
import networkx as nx
import Queue
import csv
import sys
import time

def Betweenness(root):

	#BFS from root
	bfs = nx.DiGraph()
	level = [-1] * nodeCount
	shortestPathCount = [0] * nodeCount
	queue = Queue.Queue()

	queue.put(root)
	level[root] = 0
	shortestPathCount[root] = 1

	while not queue.empty():
		current = queue.get()
		currentLevel = level[current]
		currentSPC = shortestPathCount[current]

		for i in list(graph.adj[current]):
			if level[i] == -1:
				queue.put(i)
				level[i] = currentLevel + 1
				bfs.add_edge(i, current)
				shortestPathCount[i] = shortestPathCount[i] + currentSPC

			elif level[i] == currentLevel + 1:
				bfs.add_edge(i, current)
				shortestPathCount[i] = shortestPathCount[i] + currentSPC

	#Level-wise info
	levelNodesDict = {}
	for i in range(len(level)):
		key = level[i]
		if key not in levelNodesDict:
			levelNodesDict[key] = [i]
		else:
			levelNodesDict[key].append(i)

	#Betweenness
	edgeWtDict = {}
	nodeWt = [float(1)] * nodeCount
	maxLevel = max(level)

	currentLevel = maxLevel

	while currentLevel >= 0:
		currentNodes = levelNodesDict[currentLevel]
		for curNode in currentNodes:
			inEdges = list(bfs.in_edges(curNode))
			for inEdge in inEdges:
				nodeWt[curNode] = nodeWt[curNode] + edgeWtDict[inEdge]

			outEdges = list(bfs.out_edges(curNode))
			for outEdge in outEdges:
				betVal = nodeWt[curNode] / shortestPathCount[curNode]
				edgeWtDict[outEdge] = betVal
				yield (outEdge, betVal)

		currentLevel = currentLevel - 1


start = time.time()
ratingsFile = sys.argv[1]

sc = SparkContext()
rdd = sc.textFile(ratingsFile, minPartitions=None, use_unicode=False)
rdd = rdd.mapPartitions(lambda x : csv.reader(x))
header = rdd.first()
rdd = rdd.filter(lambda x : x != header)

data = rdd.map(lambda x : (int(x[0]) - 1, int(x[1]))).groupByKey().mapValues(set)
data = data.cartesian(data).filter(lambda x : x[0][0] < x[1][0])
userPairs = data.map(lambda x : ((x[0][0], x[1][0]), x[0][1].intersection(x[1][1])))
graphUserPairs = userPairs.filter(lambda x : len(x[1]) >= 9).map(lambda x : x[0])

#Creating graph
edges = graphUserPairs.collect()
graph = nx.Graph()
graph.add_edges_from(edges)

nodeList = list(graph.nodes)
nodeCount = len(nodeList)
nodesRdd = sc.parallelize(nodeList)

#Betweenness
results = nodesRdd.flatMap(lambda x : (Betweenness(x))).map(lambda x : (tuple(sorted(x[0])), x[1]))
results = results.reduceByKey(lambda x, y : x + y).sortByKey().map(lambda x : ((x[0][0], x[0][1]), x[1]/2))

#Initial modularity calculation
originalGraph = nx.Graph(graph)
edgeCount = len(edges)
denominator = float(2 * edgeCount)
nodeDegreeRdd = nodesRdd.map(lambda x : (x, originalGraph.degree[x]))
nodeDegreeJoin = nodeDegreeRdd.cartesian(nodeDegreeRdd)
nodeDegreeJoin = nodeDegreeJoin.map(lambda x : ((x[0][0], x[1][0]), (x[0][1] * x[1][1])/denominator))

term2 = nodeDegreeJoin.values().sum()
term1 = 2 * edgeCount
modularity = (term1 - term2) / denominator


betEdgeList = results.map(lambda x : (x[1]//0.001/1000, x[0])).groupByKey().mapValues(list).sortByKey(False).collect()
edgeDict = sc.parallelize(edges).map(lambda x: (x,1)).sortByKey(True).collectAsMap()
nodeDegreeJoinDict = nodeDegreeJoin.collectAsMap()

#Removing edges and re-calculating modularity
i = 0
numberOfConnectedComponents = 1
maxModularity = modularity
maxModularityGraph = nx.Graph(graph)
while i < len(betEdgeList):

	edgesToBeRemoved = betEdgeList[i][1]
	graph.remove_edges_from(edgesToBeRemoved)

	newNumberOfConnectedComponents = nx.algorithms.components.number_connected_components(graph)
	if newNumberOfConnectedComponents == numberOfConnectedComponents:
		i = i + 1
		continue

	modularity = 0
	connectedComponents = list(nx.algorithms.components.connected_components(graph))

	for component in connectedComponents:
		nodes = list(component)
		for node1 in nodes:
			for node2 in nodes:
				if((node1, node2) in edgeDict or (node2, node1) in edgeDict):
					term1 = 1
				else:
					term1 = 0

				term2 = nodeDegreeJoinDict[(node1,node2)]
				modularity = modularity + (term1 - term2)

	modularity = modularity / denominator
	
	if modularity > maxModularity:
		maxModularity = modularity
		maxModularityGraph = nx.Graph(graph)

	numberOfConnectedComponents = newNumberOfConnectedComponents
	i = i + 1

finalResult = list()
communities = list(nx.algorithms.components.connected_components(maxModularityGraph))
for community in communities:
	community = sorted(list(community))
	temp = [x + 1 for x in community]
	finalResult.append(temp)
finalResult = sorted(finalResult)

outputFile = "Nupur_Shukla_Community.txt"
outFile = open(outputFile, "w")
for community in finalResult:
	outFile.write(str(community).replace(" ", ""))
	outFile.write("\n")
outFile.close()

sc.stop()
end = time.time()
print "Time: ", end - start, " seconds"
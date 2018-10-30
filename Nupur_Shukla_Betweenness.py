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
nodes = sc.parallelize(nodeList)

#Algorithm
results = nodes.flatMap(lambda x : (Betweenness(x))).map(lambda x : (tuple(sorted(x[0])), x[1]))
results = results.reduceByKey(lambda x, y : x + y).sortByKey().map(lambda x : (x[0][0] + 1, x[0][1] + 1, x[1]/2))
finalList = results.collect()

outputFile = "Nupur_Shukla_Betweenness.txt"
outFile = open(outputFile, "w")
for i in range(len(finalList)):
	string = "(" + str(finalList[i][0]) + "," + str(finalList[i][1]) + "," + str(finalList[i][2]) + ")\n"
	outFile.write(string)
outFile.close()

end = time.time()
print "Time: ", end - start, " seconds"
from pyspark import SparkContext
import networkx as nx
import csv
import sys
import time

start = time.time()
ratingsFile = sys.argv[1]

sc = SparkContext()
rdd = sc.textFile(ratingsFile, minPartitions=None, use_unicode=False)
rdd = rdd.mapPartitions(lambda x : csv.reader(x))
header = rdd.first()
rdd = rdd.filter(lambda x : x != header)

data = rdd.map(lambda x : (int(x[0]), int(x[1]))).groupByKey().mapValues(set)
data = data.cartesian(data).filter(lambda x : x[0][0] < x[1][0])
userPairs = data.map(lambda x : ((x[0][0], x[1][0]), x[0][1].intersection(x[1][1])))
graphUserPairs = userPairs.filter(lambda x : len(x[1]) >= 9).map(lambda x : x[0])

#Creating graph
edges = graphUserPairs.collect()
graph = nx.Graph()
graph.add_edges_from(edges)

#Library to get 4 communities
comp = nx.algorithms.community.centrality.girvan_newman(graph)
i1 = next(comp)
i2 = next(comp)
i3 = next(comp)
finalResult = sorted(list(sorted(c) for c in i3))

#Writing result to file
outputFile = "Nupur_Shukla_Bonus.txt"
outFile = open(outputFile, "w")
for community in finalResult:
	outFile.write(str(community).replace(" ", ""))
	outFile.write("\n")
outFile.close()

sc.stop()
end = time.time()
print "Time: ", end - start, " seconds"
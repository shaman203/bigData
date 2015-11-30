library(igraph)
el=read.csv("edges") #read edgelist file (must have header, 3 columns, last called weight)
g=graph.data.frame(el,directed = FALSE)

#adj=get.adjacency(g,attr='weight') #attr='weight' makes sure that the weights are shown in the adjacency matrix.
#adj

groups <- fastgreedy.community(g) # and voila

groupAssigments <- cbind(groups$names, groups$membership, deparse.level = 0)
colnames(groupAssigments) <- c("vertex", "group")
write.csv(groupAssigments, file="blogGroups.csv")

#par(mar=c(1,1,1,1)) #plot stupidity rectifier
#plot(g,edge.width=E(g)$weight/2, vertex.size = 0)

groupAssigments <- read.csv("blogGroups.csv")
groupEdgeList <- cbind(groupAssigments[match(el[,1],groupAssigments[,2]),3],groupAssigments[match(el[,2],groupAssigments[,2]),3], el[,3])


groupedEdges <- aggregate(groupEdgeList[,3], list(groupEdgeList[,1],groupEdgeList[,2]), FUN=mean)
names(groupedEdges) <- c("V1", "V2", "weight")

#groupedEdges2 <- groupedEdges[groupedEdges[,1] < 12,]
g2 <- graph.data.frame(groupedEdges,directed = TRUE)

#remove solitary edges(  degree <= 2)

g2 <- simplify(g2)
ids <- degree(g2);
#g2 <- delete.edges(g2,E(g2)[names(ids[ids <= 2]) %--% names(ids[ids <= 2])])
g2 <- delete.vertices(g2, V(g2)[names(ids[ids == 0])])



plot(g2, edge.width=E(g2)$weight/2)

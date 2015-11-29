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
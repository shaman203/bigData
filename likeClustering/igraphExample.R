el=read.csv(file.choose()) #read edgelist file (must have header, 3 columns, last called weight)
g=graph.data.frame(el,directed = FALSE)
par(mar=c(1,1,1,1)) #plot stupidity rectifier
plot(g,layout=layout.fruchterman.reingold,edge.width=E(g)$weight/2)

adj=get.adjacency(g,attr='weight') #attr='weight' makes sure that the weights are shown in the adjacency matrix.
adj

fastgreedy.community(g) # and voila

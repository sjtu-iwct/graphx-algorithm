<html>
<body>
#GraphX Operators

##Fundamental Infomation Interface
###Function:
Get information about the graph

	val numEdges: Long
	val numVertices: Long
	val inDegrees: VertexRDD[Int]
	val outDegrees: VertexRDD[Int]
	val degrees: VertexRDD[Int]

###Brief Explanatioin:
<p>1.numEdges returns the number of the edges in the graph.</p>
<p>2.numVertices returns the number of the vertices in the graph.</p>
<p>3.inDegrees, outDegrees and degrees are three types of the countings on the degrees of the vertices. inDegrees returns in-degree of each vertex.
outDegrees returns out-degree of each vertex.
degree is the sum of inDegrees and OutDegrees.Note that Vertices whose in-degree are 0 won't be included in the result RDD. </p>
<p>GraphOps calls method degreesRDD to compute the neighboring vertex degrees.</p>

	@transient lazy val inDegrees: VertexRDD[Int] = degreesRDD(EdgeDirection.In).setName("GraphOps.inDegrees")
	@transient lazy val outDegrees: VertexRDD[Int] = degreesRDD(EdgeDirection.Out).setName("GraphOps.outDegrees")
	@transient lazy val degrees: VertexRDD[Int] = degreesRDD(EdgeDirection.Either).setName("GraphOps.degrees")
 
	private def degreesRDD(edgeDirection: EdgeDirection): VertexRDD[Int] = {
		if (edgeDirection == EdgeDirection.In) {
    		graph.aggregateMessages(_.sendToDst(1), _ + _, TripletFields.None)
		} else if (edgeDirection == EdgeDirection.Out) {
    		graph.aggregateMessages(_.sendToSrc(1), _ + _, TripletFields.None)
		} else { // EdgeDirection.Either
    		graph.aggregateMessages(ctx => { ctx.sendToSrc(1); ctx.sendToDst(1) }, _ + _, TripletFields.None)
		}
	}

###Using Method:
<p>Now, let's start a simple test. Firstly, we will create a graph by GraphX.</p>

	import org.apache.spark._
	import org.apache.spark.graphx._
	import org.apache.spark.rdd.RDD
	val graph = GraphLoader.edgeListFile(sc, "hdfs://192.168.17.240:9222/input/yuhc/web-Google/web-Google.txt")

In the following tutorial, we will repeatedly use this graph. To be brief, we won't refer to it again.
<p>1.numEdges operator: </p>

	scala> val tmp = graph.numEdges
	tmp: Long = 5105039

<p>2.numVertices operator: </p>

	scala> val tmp = graph.numVertices
	tmp: Long = 875713

<p>3.degrees operator: </p>

	scala> val tmp = graph.inDegrees
	scala> tmp.take(10)
	res0: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,2), (672890,1), (129434,2), (194402,2), (199516,28), (332918,3), (170792,1), (386896,3), (691634,71), (291526,9))

	scala> val tmp = graph.outDegrees
	scala> tmp.take(10)
	res1: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,1), (129434,2), (194402,1), (199516,20), (332918,3), (170792,9), (386896,18), (691634,11), (291526,7), (513652,1))

	scala> val tmp = graph.degrees
	scala> tmp.take(10)
	res0: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,3), (672890,1), (129434,4), (194402,3), (199516,48), (332918,6), (170792,10), (386896,21), (691634,82), (291526,16))

###Ending:	
<p>These five operators are fundamental tools to check graph's attributes.</p>



##Property Operators
###Function:
Transform vertex and edge attributes

	def mapVertices[VD2](map: (VertexID, VD) => VD2): Graph[VD2, ED]
	def mapEdges[ED2](map: Edge[ED] => ED2): Graph[VD, ED2]
	def mapEdges[ED2](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2]
	def mapTriplets[ED2](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]
	def mapTriplets[ED2](map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2]): Graph[VD, ED2]

###Brief Explanation:
<p>These five functions are divided into three groups, map to vertices, map to edges and map to triplets. They respectively transform the attributes of vertices, edges and triplets.</p>
<p>1.mapVertices can transform each vertex attribute in graph. map is the function from a vertex object to a new vertex value. The new graph structure is the same as the old one, so the underlying index structures can be reused. VD2 is the new vertex data type.</p>
<p>2.mapEdges can transform each edge attribute in graph.
mapEdges has two duplicate functions:</p>

	def mapEdges[ED2](map: Edge[ED] => ED2): Graph[VD, ED2]
	def mapEdges[ED2](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2]

<p>The first one call the second to finish the maping process. Note that it doesn't pass the vertex value for the vertices adjacent to the edge (while mapTriplets does).  The new graph has the same structure as the old one.</p>
<p>3.mapTriplets can transform each triplet attribute in graph. mapTriplets is similar to those two functions. We will use it in practice to know how it works.</p>

###Using Method:
<p>1.mapVeritces operator: </p>
Take ten nodes from the graph and results:

	scala> graph.vertices.take(10)
	res1: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,1), (672890,1), (129434,1), (194402,1), (199516,1), (332918,1), (170792,1), (386896,1), (691634,1), (291526,1))

Each vertex’s value is set to 1, which is a default operation. We can set the values to 2 by:

	scala> val tmp = graph.mapVertices((id, attr) => attr.toInt * 2)

If we use tmp.vertices.take(10) to see the values, it returns

	scala> tmp.vertices.take(10)
	res2: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,2), (672890,2), (129434,2), (194402,2), (199516,2), (332918,2), (170792,2), (386896,2), (691634,2), (291526,2))

Another optimized method is:

	scala> val tmp :Graph[Int, Int] = graph.mapVertices((_, attr) => attr * 3)

It contains

	res3: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,3), (672890,3), (129434,3), (194402,3), (199516,3), (332918,3), (170792,3), (386896,3), (691634,3), (291526,3))

<p>2.mapEdges operator: </p>
Take ten edges from the graph and results:

	scala> graph.edges.take(10)

It returns:

	res1: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(0,11342,1), Edge(0,824020,1), Edge(0,867923,1), Edge(0,891835,1), Edge(1,53051,1), Edge(1,203402,1), Edge(1,223236,1), Edge(1,276233,1), Edge(1,552600,1), Edge(1,569212,1))

Multiply the edges’ attributes by 2:

	scala> val tmp = graph.mapEdges(e => e.attr.toInt * 2)

Then tmp.edges.take(10) returns:

	res2: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(0,11342,2), Edge(0,824020,2), Edge(0,867923,2), Edge(0,891835,2), Edge(1,53051,2), Edge(1,203402,2), Edge(1,223236,2), Edge(1,276233,2), Edge(1,552600,2), Edge(1,569212,2))

<p>3.mapTriplets operator: </p>
Take ten triplets from the graph and results:

	scala> graph.triplets.take(10)

It returns:

	res1: Array[org.apache.spark.graphx.EdgeTriplet[Int,Int]] = Array(((0,1),(11342,1),1), ((0,1),(824020,1),1), ((0,1),(867923,1),1), ((0,1),(891835,1),1), ((1,1),(53051,1),1), ((1,1),(203402,1),1), ((1,1),(223236,1),1), ((1,1),(276233,1),1), ((1,1),(552600,1),1), ((1,1),(569212,1),1))

Type in the following code, which sets the edge value to the sum of twice the source vertex value and three times the destination vertex value.

	scala> val tmp = graph.mapTriplets(et => et.srcAttr.toInt * 2 + et.dstAttr.toInt * 3)

Then tmp.triplets.take(10) returns:

	res2: Array[org.apache.spark.graphx.EdgeTriplet[Int,Int]] = Array(((0,1),(11342,1),5), ((0,1),(824020,1),5), ((0,1),(867923,1),5), ((0,1),(891835,1),5), ((1,1),(53051,1),5), ((1,1),(203402,1),5), ((1,1),(223236,1),5), ((1,1),(276233,1),5), ((1,1),(552600,1),5), ((1,1),(569212,1),5))

###Ending:
<p>These three attributes tranformer can modify the edges and vertices attributes in the new graph while retain the original one which will be very useful in practice.</p>



##Structual Operators
###Function:
Modify the graph structure

	class Graph[VD, ED] {
		def reverse: Graph[VD, ED]
		def subgraph(epred: EdgeTriplet[VD,ED] => Boolean,
               		 vpred: (VertexId, VD) => Boolean): Graph[VD, ED]
		def mask[VD2, ED2](other: Graph[VD2, ED2]): Graph[VD, ED]
		def groupEdges(merge: (ED, ED) => ED): Graph[VD,ED]
	}

###Brief Explanatioin:
<p>1.The reverse operator returns a new graph with all the edge directions reversed. Note that the reverse operation does not modify vertex or edge properties or change the number of edges. So it can be implemented efficiently without data movement or duplication. Such fuction is useful, for example,  when computing inverse PageRank.</p>
<p>2.The subgraph operator returns the graph containing only the vertices and edges that satisfy the predicates. Noted that the predicate takes a vertex object or a triplet and evaluates to true. </p>
<p>Also notice that only edges where both vertices satisfy the vertex predicate are considered.</p>

	V' = {v : for all v in V where vpred(v)}
	E' = {(u,v): for all (u,v) in E where epred((u,v)) && vpred(u) && vpred(v)}

<p>The subgraph operator can be used in number of situations to restrict the graph to the vertices and edges of interest or eliminate broken links.</p>
<p>3.The mask operator returns a subgraph of the current graph that contains the vertices and edges that are also found in the input graph. This can be used in conjunction with the subgraph operator to restrict a graph based on the properties in another related graph.</p>
For example, we might run connected components using the graph with missing vertices and then restrict the answer to the valid subgraph. 
<p>4.The groupEdges operator merges parallel edges (i.e., duplicate edges between pairs of vertices) into a single edge in the multigraph. In many numerical applications, parallel edges can be added (their weights combined) into a single edge thereby reducing the size of the graph.</p>

###Using Method:
<p>1.reverse operator: </p>

	scala> val tmp = graph.edges.take(10)
	tmp: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(0,11342,1), Edge(0,824020,1), Edge(0,867923,1), Edge(0,891835,1), Edge(1,53051,1), Edge(1,203402,1), Edge(1,223236,1), Edge(1,276233,1), Edge(1,552600,1), Edge(1,569212,1))
	scala> tmp.reverse
	res1: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(1,569212,1), Edge(1,552600,1), Edge(1,276233,1), Edge(1,223236,1), Edge(1,203402,1), Edge(1,53051,1), Edge(0,891835,1), Edge(0,867923,1), Edge(0,824020,1), Edge(0,11342,1))

We can find the SrcID and DstID in the edge attributes were exchanged.

<p>2.subgraph operator: </p>
Create subgraph:

	scala> val subgraph = graph.subgraph(epred = e => e.srcId > e.dstId)

Count the vertices and edges in the subgraph:

	scala> subgraph.vertices.count
	res1: Long = 875713
	scala> subgraph.edges.count
	res2: Long = 2420548
	scala> subgraph.edges.take(10)
	res3: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(1122,429,1), Edge(1300,606,1), Edge(1436,409,1), Edge(1509,1401,1), Edge(1513,1406,1), Edge(1624,827,1), Edge(1705,693,1), Edge(1825,1717,1), Edge(1985,827,1), Edge(2135,600,1))

Only edges whose source vertex ID is larger than destination vertex ID are left.
Take vertices predicate into account:

	scala> val subgraph = graph.subgraph(epred = e => e.srcId > e.dstId, vpred = (id, _) => id > 500000)

Count the vertices and edges in the new subgraph:

	scala> subgraph.vertices.count
	res1: Long = 400340
	scala> subgraph.edges.count
	res2: Long = 526711
	scala> subgraph.edges.take(10)
	res3: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(500397,500290,1), Edge(500627,500542,1), Edge(501011,500055,1), Edge(501663,500010,1), Edge(501941,501177,1), Edge(501984,501223,1), Edge(502067,500279,1), Edge(502187,500532,1), Edge(502335,500279,1), Edge(502809,500538,1))

It realized the function of filter for the vertices restriction.
<p>3.mask opertor: </p>
For example, we might run connected components using the graph with missing vertices and then restrict the answer to the valid subgraph.

	// Run Connected Components
	scala> val ccGraph = graph.connectedComponents() // No longer contains missing field
	// Remove missing vertices as well as the edges to connected to them
	scala> val validGraph = graph.subgraph(vpred = (id, attr) => id > 500000)
	// Restrict the answer to the valid subgraph
	scala> val validCCGraph = ccGraph.mask(validGraph)
	scala> validCCGraph.vertices.take(10)
	res1: Array[(org.apache.spark.graphx.VertexId, org.apache.spark.graphx.VertexId)] = Array((672890,0), (691634,0), (513652,0), (620402,0), (806938,0), (637370,0), (605508,0), (884124,0), (769382,0), (806480,0))

We can find that we successfully realized our target. The new subgraph is from the original graph while it also has the property from the input graph.
<p>4.groupEdges: </p>

	scala> val subgraph = graph.groupEdges((a, b) => a+b)

###Ending:
<p>Different from Property Operators, Structual Operators are means to change the structure of the graph. So it's also an very important tool in GraphX.</p>



##Join Operators
###Function:
Join RDDs with the graph

	def joinVertices[U](table: RDD[(VertexID, U)])(mapFunc: (VertexID, VD, U) => VD): Graph[VD, ED]
	def outerJoinVertices[U, VD2](other: RDD[(VertexID, U)])
    	(mapFunc: (VertexID, VD, Option[U]) => VD2)
      : Graph[VD2, ED]

###Brief Explanatioin:
<p>1.In many cases it is necessary to join data from external collections (RDDs) with graphs. For example, we might have extra user properties that we want to merge with an existing graph or we might want to pull vertex properties from one graph into another. Such tasks will be accomplished by joinVertices function. </p>

	/**
	 * @example This function is used to update the vertices with new
	 * values based on external data. For example we could add the out
	 * degree to each vertex record
	 *
	 * {{{
	 * val rawGraph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "webgraph").mapVertices((_, _) => 0)
	 * val outDeg = rawGraph.outDegrees
	 * val graph = rawGraph.joinVertices[Int](outDeg)
	 *   ((_, _, outDeg) => outDeg)
	 * }}}
	 */
	def joinVertices[U: ClassTag](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD)
	  : Graph[VD, ED] = {
	  val uf = (id: VertexId, data: VD, o: Option[U]) => {
    	o match {
      	  case Some(u) => mapFunc(id, data, u)
      	  case None => data
    	}
	  }
	  graph.outerJoinVertices(table)(uf)
	}

<p>The joinVertices operator joins the vertices with the input RDD table and returns a new graph with the vertex properties obtained by applying map function to the result of the joined vertices. Vertices without a matching value in the RDD retain their original value.</p>
<p>2.The more general outerJoinVertices behaves similarly to joinVertices. Unlike that joinVertices sets a default action, the map function in outerJoinVertices takes an option type for those vertices which don't have a matching value. For example, we can setup a graph for PageRank by initializing vertex properties with their outDegree.</p>

	/**
	 * @example This function is used to update the vertices with new values based on external data.
	 *          For example we could add the out-degree to each vertex record:
	 *
	 * {{{
	 * val rawGraph: Graph[_, _] = Graph.textFile("webgraph")
	 * val outDeg: RDD[(VertexId, Int)] = rawGraph.outDegrees
	 * val graph = rawGraph.outerJoinVertices(outDeg) {
	 *   (vid, data, optDeg) => optDeg.getOrElse(0)
	 * }
	 * }}}
	 */
	def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
    (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
	  : Graph[VD2, ED]


###Using Method:
<p>1.joinVertices operator:　</p>
We joined outDeg to rawGraph. 

	scala> val rawGraph = graph.mapVertices((id, attr) => 0)
	scala> val outDeg = rawGraph.outDegrees
	scala> val tmp = rawGraph.joinVertices[Int](outDeg)((_, _, optDeg) => optDeg)
	scala> outDeg.take(5)
	res1: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,1), (129434,2), (194402,1), (199516,20), (332918,3))
	scala> tmp.vertices.take(5)
	res2: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,1), (672890,0), (129434,2), (194402,1), (199516,20))

<p>2.outerJoinVertices operator: </p>
Compared to joinVertics, we will add a case to realize the option in outerJoinVertices.

	scala> val tmp = rawGraph.outerJoinVertices[Int, Int](outDeg)((_, _, optDeg) => optDeg.getOrElse(0))
	// Also canbe written in this way
	//val tmp = rawGraph.outerJoinVertices[Int, Int](outDeg){(_, _, optDeg) => optDeg match {
	//	case Some(outDeg) => outDeg
	//	case None => 0
	//	}
	//}
	scala> outDeg.take(5)
	res1: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,1), (129434,2), (194402,1), (199516,20), (332918,3))
	scala> tmp.vertices.take(5)
	res2: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((354796,1), (672890,0), (129434,2), (194402,1), (199516,20))

###Ending:
Join operators are designed to upgrade the graph by input RDD table. It provides us another way to modify the vertices' porperty.



##Neighborhood Aggregation
###Function:
Aggregate information about adjacent triplets

	def collectNeighborIds(edgeDirection: EdgeDirection): VertexRDD[Array[VertexID]]
	def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexID, VD)]]
	def aggregateMessages[Msg: ClassTag](
		sendMsg: EdgeContext[VD, ED, Msg] => Unit,
      	mergeMsg: (Msg, Msg) => Msg,
      	tripletFields: TripletFields = TripletFields.All)
      : VertexRDD[A]

###Brief Explanatioin:
<p>1.The collectNeighborIds operator returns the set of neighboring IDs for each vertex. Note that neither collectNeighborIds (nor collectNeighbors) doesn't support EdgeDirection.Both.</p>
<p>2.The collectNeighbors returns the vertex set of neighboring vertex attributes for each vertex. Note that  it could be highly inefficient on power-law graphs where high degree vertices may force a large amount of information to be collected to a single location.</p>
</p>3.The core aggregation operation in GraphX is aggregateMessages. This operator applies a user defined sendMsg function to each edge triplet in the graph and then uses the mergeMsg function to aggregate those messages at their destination vertex.</p>
<p>The user defined sendMsg function takes an EdgeContext, which exposes the source and destination attributes along with the edge attribute and functions to send messages to the source and destination attributes. The user defined mergeMsg function takes two messages destined to the same vertex and yields a single message. The aggregateMessages operator returns a VertexRDD[Msg] containing the aggregate message with type Msg destined to each vertex. Vertices that did not receive a message are not included in the returned VertexRDD.</p>
<p>Think of sendMsg as the map function and think of mergeMsg as the reduce function. aggregateMessages operator runs in a similar way with mapreduce.
In addition, aggregateMessages takes an optional tripletsFields which indicates what data is accessed in the EdgeContext.</p>

###Using Method:
<p>1.collectNeighborIds operator: </p>
We want to get the set of out-direction neighboring IDs for each vertex.

	scala> val tmp = graph.collectNeighborIds(EdgeDirection.Out)
	scala> tmp.take(10)
	res1: Array[(org.apache.spark.graphx.VertexId, Array[org.apache.spark.graphx.VertexId])] = Array((354796,Array(798944)), (672890,Array()), (129434,Array(110771, 119943)), (194402,Array(359291)), (199516,Array(26483, 190323, 193759, 280401, 329066, 342523, 367518, 398314, 417194, 427451, 458892, 459074, 485460, 502995, 505260, 514621, 660407, 798276, 810885, 835966)), (332918,Array(12304, 89384, 267989)), (170792,Array(227187, 255153, 400178, 453412, 512326, 592923, 663311, 666734, 864151)), (386896,Array(109021, 155460, 200406, 204397, 282107, 378570, 427843, 602779, 616132, 629079, 669605, 717650, 727162, 761159, 796410, 832809, 890838, 891178)), (691634,Array(13996, 32163, 33185, 39682, 193103, 197677, 520483, 598034, 727805, 747975, 836657)), (291526,Array(206053, 271366, 383159, 418...

<p>2.collectNeighbors operator: </p>
We want to get the vertex set of neighboring vertex attributes for each vertex instead of the vertex IDs.

	scala> val tmp = graph.collectNeighbors(EdgeDirection.Out)
	scala> tmp.take(10)
	res1: Array[(org.apache.spark.graphx.VertexId, Array[(org.apache.spark.graphx.VertexId, Int)])] = Array((354796,Array((798944,1))), (672890,Array()), (129434,Array((110771,1), (119943,1))), (194402,Array((359291,1))), (199516,Array((26483,1), (190323,1), (193759,1), (280401,1), (329066,1), (342523,1), (367518,1), (398314,1), (417194,1), (427451,1), (458892,1), (459074,1), (485460,1), (502995,1), (505260,1), (514621,1), (660407,1), (798276,1), (810885,1), (835966,1))), (332918,Array((12304,1), (89384,1), (267989,1))), (170792,Array((227187,1), (255153,1), (400178,1), (453412,1), (512326,1), (592923,1), (663311,1), (666734,1), (864151,1))), (386896,Array((109021,1), (155460,1), (200406,1), (204397,1), (282107,1), (378570,1), (427843,1), (602779,1), (616132,1), (629079,1), (669605,1), (717...

<p>3.aggregateMessages operator: </p>
Suppose the vertex IDs are the ages of the people. We want to find the number of people who are older than each person, and the average age of those older people using aggregateMessages
Firstly, we generate a graph with 100 vertices.

	// Include Corresponding Classes
	scala> import org.apache.spark._
	scala> import org.apache.spark.graphx._
	scala> import org.apache.spark.rdd.RDD
	scala> import org.apache.spark.graphx.util.GraphGenerators
	// Generate Graph while modifying the attributes
	scala> GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
	scala> val graph: Graph[Double, Int] =
     | GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
	//Out put some samples.
	scala> graph.vertices.take(10)
	res1: Array[(org.apache.spark.graphx.VertexId, Double)] = Array((84,84.0), (96,96.0), (52,52.0), (56,56.0), (4,4.0), (76,76.0), (16,16.0), (28,28.0), (80,80.0), (48,48.0))
	scala> graph.edges.take(10)
	res2: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(0,0,1), Edge(0,1,1), Edge(0,1,1), Edge(0,2,1), Edge(0,3,1), Edge(0,3,1), Edge(0,7,1), Edge(0,11,1), Edge(0,15,1), Edge(0,21,1))

Then, we use aggregateMessages to calculate the target.

	scala> val olderPeople: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)] (
     | triplet => { //Map Function
     |   if (triplet.srcAttr > triplet.dstAttr) {
     |     triplet.sendToDst(1, triplet.srcAttr)
     |   }
     | },
     | //Reduce Function
     | (a, b) => (a._1 + b._1, a._2 + b._2)
     | )
	scala> val avgAgeOfOlderPeople: VertexRDD[Double] =
     | olderPeople.mapValues( (id, value) => value match { case (count, totalAge) => totalAge/count } )
	scala> avgAgeOfOlderPeople.collect.foreach(println(_))
	(84,91.42857142857143)
	(96,97.0)
	(52,78.8695652173913)
	(56,74.64285714285714)
	(4,45.725)
	(76,86.3)
	(16,52.2)
	(28,65.58064516129032)
	(80,89.25)
	(48,80.05)
	...

###Ending:
Among the three operators, aggregateMessages is the most important. It's a evolution from mapreduceTriplet which has been replaced. And most algorithms' implementation will depend on it.



##Cache Operators
###Fuction:
Cache the graphs

	def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]
	def cache(): Graph[VD, ED]
	def unpersistVertices(blocking: Boolean = true): Graph[VD, ED]

###Brief Explanatioin:
In Spark, RDDs are not persisted in memory by default. To avoid recomputation, they must be explicitly cached when using them multiple times. Graphs in GraphX behave the same way. **When using a graph multiple times, make sure to call Graph.cache() on it first.**

###Using Method:

	Graph.cache()

###Ending:
It's a common tools to accelerate the speed and save the time.



##Reference
1.[GraphX Programming Guide](http://spark.apache.org/docs/latest/graphx-programming-guide.html#graph-operators)

2.[Graph Operators in GraphX – Part 1](http://note.yuhc.me/2015/03/graphx-property-operator-1/)

3.[Graph Operators in GraphX – Part 2](http://note.yuhc.me/2015/03/graphx-property-operator-2/)


</body>
</html>
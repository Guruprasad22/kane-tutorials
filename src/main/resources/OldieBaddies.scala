val movieNamesRdd = sc.textFile("u.item")
val nameLookup = movieNamesRdd.map(x => (x.split("\\|")(0),x.split("\\|")(1))).collectAsMap
val movieData = sc.textFile("u.data")
val movieDataRefined = movieData.map(x => (x.split("\t")(1),(x.split("\t")(2).toInt,1.0)))
val movieDataAgg = movieDataRefined.reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))
val movieDataAvg = movieDataAgg.mapValues(x => (x._1/x._2).toFloat)
val sortedResult = movieDataAvg.sortBy(x => x._2)
val sample = sortedResult.take(10)
for( result <- sample) println(nameLookup(result._1),result._2)
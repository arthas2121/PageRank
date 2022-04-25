import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
           
         //links.foreach(println)
           
        val outlinks = links.map{link =>
        (link.split(':')(0).toInt, link.split(": ")(1).split(' ').map(x =>x.toInt))}

	//outlinks.foreach(println)
	
	val titles = sc
            .textFile(titles_file, num_partitions)
       
        val title_map = titles.zipWithIndex()
             .map{case (title, index) => ((index + 1).toInt, title)}
       
        //title_map.foreach(println)
       
        /* PageRank */
       
        val N = title_map.count
        var old_PR = title_map.mapValues(title => 100.0 / N)
        var d = 0.85

        var new_PR = old_PR
             
        //should be iters here ***
        
        for (i <- 1 to iters) {
        
            //get the individual ranks for each sub-outlink in one link.
            val out = old_PR.join(outlinks).values
            .flatMap{case (value, outs) =>
            outs.map(num => (num, value / outs.size))}
            
            //out.foreach(println)
            
            //find the pagerank of the ones that have outlinks.
            new_PR = out.reduceByKey(_+_).mapValues(d*_ + ((1-d)*100.0/N))                 
            //find the pagerank of the ones that are left.
            val left_PR = old_PR.subtractByKey(new_PR).mapValues(x=>(1-d)*100.0/N)
            
            //combine them to be the old_PR and update every time.
            old_PR = new_PR ++ left_PR
            
        }
       
       
        println("[ PageRanks ]")
       
        //find the pagerank with the sum of all of them to 100.
        val rank_sum = old_PR.map(x => x._2).sum()
        val final_rank = old_PR.mapValues(x=>x*100/rank_sum)
        
        //join the final result to be the same format of the output.
        val final_PR = title_map.join(final_rank)
        
        //sort by the pagerank value and take the first ten elements.
        final_PR.sortBy(x => x._2._2, false).take(10).foreach(println)
        
    }
}

package pairs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import scala.Tuple2;


public class BasicTest {
	

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("BasicTest")
        .master("local[4]")
        .getOrCreate();

    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    
    List<String> data = Arrays.asList("group1","group2"); //groupBy
    Dataset<String> groups = spark.createDataset(data, Encoders.STRING());
    
    
    List<Name> nameList =  Arrays.asList(new Name("hi"),new Name("bye")); //greating
    Encoder<Name> nameEncoder =  Encoders.bean(Name.class);
    Dataset<Name> nameDs = spark.createDataset(nameList,nameEncoder);
    
    Dataset<Name> rsDs = groups.map( (MapFunction<String,  Name>) code -> {
    	    System.out.println(" code :" + code);
    	    
    	   return calcFunction(spark, nameDs, code);
    	} ,nameEncoder);
    	.reduce(  (ds1, ds2) -> {
    		
    		return ds1.union(ds2);
    		
    		/*List<Name> ll = new ArrayList<>();
    		ll.add(ds1);
    		ll.add(ds2);
    		
    		return ll;*/
    		
    	},nameEncoder));


    spark.stop();
  }
  
  public static Dataset<Name> calcFunction(SparkSession sparkSession, Dataset<Name> ds , String x_code ){
         //this is actually a complex logic , for simplicity written like this
	  Dataset<Name> ds_res = 
    		           ds.withColumn("codeName", concat(col("codeName"), lit("_"),lit(x_code)))
    		             .as(Encoders.bean(Name.class));
           //write
      
      return ds_res ; 
  }
  
}

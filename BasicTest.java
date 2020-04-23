import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import dataset.JavaBean.Number;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import scala.Tuple2;


public class BasicTest {
	

public static class  Name implements Serializable {
    private String codeName;
    
    public  Name(){
	}
	public Name(String n ){
		this.codeName = n;
	}

	public String getCodeName() {
		return codeName;
	}

	public void setCodeName(String codeName) {
		this.codeName = codeName;
	}
	
}

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("BasicTest")
        .master("local[4]")
        .getOrCreate();

    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    
    List<String> data = Arrays.asList("hello","world");
   // Dataset<Row> ds = spark.createDataset(data, Encoders.STRING()).toDF("type");
    
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    
    
    List<Name> nameList =  Arrays.asList(new Name("hi"),new Name("bye"));
    Encoder<Name> nameEncoder =  Encoders.bean(Name.class);
    Dataset<Name> nameDs = spark.createDataset(nameList,nameEncoder);
    
    
    Dataset<Name> rsDs = ds.map((MapFunction<String, Dataset>) x -> calcFunction(spark, nameDs,x) ,  nameEncoder);
    		//.reduce(func)
    //Getting error
    // The method map(Function1<String,U>, Encoder<U>) in the type Dataset<String> is not applicable for the arguments (MapFunction<String,Dataset>, Encoder<BasicTest.Name>)
    //a.show();
    

    spark.stop();
  }
  public static Dataset<Row> calcFunction(SparkSession sparkSession, Dataset<Name> ds , String x_cod ){
      Dataset<Row> ds_res = 
    		   ds.withColumn("codeName", concat(col("codeName"), lit("_"),lit(x_cod)));
      return ds_res ; 
  }
  
}

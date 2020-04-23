package dataset;

import org.apache.spark.sql.Dataset;

import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;


public class MapReduceScalingProblem {
	
	 
    public static class IndustryRevenue implements Serializable {
       
		private String industry_id;
		private String industry_name;
        private String country;
        private String state;
        private Integer revenue;
        private String generated_date;
        
        public IndustryRevenue(String industry_id, String industry_name, String country, String state, Integer revenue,
				String generated_date) {
			super();
			this.industry_id = industry_id;
			this.industry_name = industry_name;
			this.country = country;
			this.state = state;
			this.revenue = revenue;
			this.generated_date = generated_date;
		}
        
	
        public String getIndustry_id() {
			return industry_id;
		}
		public void setIndustry_id(String industry_id) {
			this.industry_id = industry_id;
		}
		public String getIndustry_name() {
			return industry_name;
		}
		public void setIndustry_name(String industry_name) {
			this.industry_name = industry_name;
		}
		public String getCountry() {
			return country;
		}
		public void setCountry(String country) {
			this.country = country;
		}
		public String getState() {
			return state;
		}
		public void setState(String state) {
			this.state = state;
		}
		public Integer getRevenue() {
			return revenue;
		}
		public void setRevenue(Integer revenue) {
			this.revenue = revenue;
		}
		public String getGenerated_date() {
			return generated_date;
		}
		public void setGenerated_date(String generated_date) {
			this.generated_date = generated_date;
		}

        
    }
    
    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("Dataset-JavaBean")
            .master("local[4]")
            .getOrCreate();
        
        Encoder<IndustryRevenue> industryRevenueEncoder = Encoders.bean(IndustryRevenue.class);
        
    	List<IndustryRevenue> data = getData();
    	
       
        Dataset<IndustryRevenue> ds = spark.createDataset(data, industryRevenueEncoder);

        System.out.println("*** here is the schema inferred from the bean");
        ds.printSchema();

        System.out.println("*** here is the data");
        ds.show();
        
        //Will get dates to for which I need to calculate , this provided by external source 
        List<String> datesToCalculate = Arrays.asList("2019-03-01","2020-06-01","2018-09-01");
        
        //Will get groups  to calculate , this provided by external source ..will keep changing
        //Have around 100s of groups.
        List<String> groupsToCalculate = Arrays.asList("Country","Country-State");
        
        //For each data given need to calculate avg(revenue) for each given group 
        //for those given each date of datesToCalculate for those records whose are later than given date.
        //i.e. 
        
        //Now I am doing some thing like this..but it is not scaling
        
        datesToCalculate.stream().forEach( cal_date -> {
        	
        	Dataset<IndustryRevenue> calc_ds = ds.where(col("generated_date").gt(lit(cal_date)));
        	
        	//this keep changing for each cal_date
        	Dataset<Row> final_ds = calc_ds
        	                          .withColumn("calc_date", to_date(lit(cal_date)).cast(DataTypes.DateType));
        	
        	//for each group it calcuate separate set
        	groupsToCalculate.stream().forEach( group -> {
        		
        		String tempViewName = new String("view_" + cal_date + "_" + group);
								
        		final_ds.createOrReplaceTempView(tempViewName);
				
				String query = "select "  
								  + " avg(revenue) as mean, "
						          + "from " + tempViewName 						
						          + " group by " + group;
				
				System.out.println("query : " + query);
				Dataset<Row> resultDs  = spark.sql(query);
				
				Dataset<Row> finalResultDs  =  resultDs
								 .withColumn("calc_date", to_date(lit(cal_date)).cast(DataTypes.DateType))
								 .withColumn("group", to_date(lit(group)).cast(DataTypes.DateType));
				
				
				//Writing to each group for each date is taking hell lot of time.
				// For each record it is save at a time
				// want to move out unioning all finalResultDs and write in batches
				finalResultDs
		           .write().format("parquet")
		           .mode("append")
		           .save("/tmp/"+ tempViewName);
				
				spark.catalog().dropTempView(tempViewName);
        		
        	});
        	
        });
        

        spark.stop();
    }
    
    /**
	 * @return
	 */
	protected static List<IndustryRevenue> getData() {
		return Arrays.asList(
				  new IndustryRevenue("Indus_1","Indus_1_Name","Country1", "State1",12789979,"2020-03-01"),
				  new IndustryRevenue("Indus_1","Indus_1_Name","Country1", "State1",56189008,"2019-06-01"),
				  new IndustryRevenue("Indus_1","Indus_1_Name","Country1", "State1",12789979,"2019-03-01"),
				  
				  new IndustryRevenue("Indus_2","Indus_2_Name","Country1", "State2",21789933,"2020-03-01"),
				  new IndustryRevenue("Indus_2","Indus_2_Name","Country1", "State2",300789933,"2018-03-01"),
				  
				  new IndustryRevenue("Indus_3","Indus_3_Name","Country1", "State3",27989978,"2019-03-01"),
				  new IndustryRevenue("Indus_3","Indus_3_Name","Country1", "State3",56189008,"2017-06-01"),
				  new IndustryRevenue("Indus_3","Indus_3_Name","Country1", "State3",30014633,"2017-03-01"),
				  
				  new IndustryRevenue("Indus_4","Indus_4_Name","Country2", "State1",41789978,"2020-03-01"),
				  new IndustryRevenue("Indus_4","Indus_4_Name","Country2", "State1",56189008,"2018-03-01"),
				  
				  new IndustryRevenue("Indus_5","Indus_5_Name","Country3", "State3",37899790,"2019-03-01"),
				  new IndustryRevenue("Indus_5","Indus_5_Name","Country3", "State3",56189008,"2018-03-01"),
				  new IndustryRevenue("Indus_5","Indus_5_Name","Country3", "State3",67789978,"2017-03-01"),
				  
				  new IndustryRevenue("Indus_6","Indus_6_Name","Country1", "State1",12789979,"2020-03-01"),
				  new IndustryRevenue("Indus_6","Indus_6_Name","Country1", "State1",37899790,"2020-06-01"),
				  new IndustryRevenue("Indus_6","Indus_6_Name","Country1", "State1",56189008,"2018-03-01"),
				  
				  new IndustryRevenue("Indus_7","Indus_7_Name","Country3", "State1",26689900,"2020-03-01"),
				  new IndustryRevenue("Indus_7","Indus_7_Name","Country3", "State1",212359979,"2020-12-01"),
				  new IndustryRevenue("Indus_7","Indus_7_Name","Country3", "State1",12789979,"2019-03-01"),
				  
				  new IndustryRevenue("Indus_8","Indus_8_Name","Country1", "State2",212359979,"2018-03-01"),
				  new IndustryRevenue("Indus_8","Indus_8_Name","Country1", "State2",26689900,"2018-09-01"),
				  new IndustryRevenue("Indus_8","Indus_8_Name","Country1", "State2",12789979,"2016-03-01"),
				  
				  new IndustryRevenue("Indus_9","Indus_9_Name","Country4", "State1",97899790,"2020-03-01"),
				  new IndustryRevenue("Indus_9","Indus_9_Name","Country4", "State1",26689900,"2019-09-01"),
				  new IndustryRevenue("Indus_9","Indus_9_Name","Country4", "State1",37899790,"2016-03-01")
				  );
	}
}

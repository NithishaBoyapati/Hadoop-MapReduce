import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*This class is used to find Min and Max Checkouts for Material Type. Also we can filter data based on year and find min and max checkouts of all material types in that year*/
public class CheckOutsMinMax {
	
	
/*Below Mapper class will split data by comma and set checkout value as min and Max value for each row in dataset
 * Also below mapper class will filter checkouts based on year. If We pass ALL as argument then it will find for all years*/
  public static class CheckOutsMapper
       extends Mapper<Object, Text, Text, MinMaxCheckOuts>{

    private Text material= new Text();
    private Integer mincheckouts;
    private Integer maxcheckouts;
    
   
    private MinMaxCheckOuts mapoutPut= new MinMaxCheckOuts();
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
// Get year value from argument
  	 String year = context.getConfiguration().get("year");
 
  	 // Split each row from dataset with comma
  	String[] checkOutFields= value.toString().split(",");
  	
  	// if check outs fields length is 9 , then we consider it as valid row other wise it as Bad data.
  	if(checkOutFields.length == 9 ) {
  	  
  		// Below condition will filter data based on year passed in argument , if it is ALL , then we consider all years data
  	  if(year != null && (year.equalsIgnoreCase("ALL") || checkOutFields[3].equalsIgnoreCase(year))){
    	
		material.set(checkOutFields[2]);
		mincheckouts=Integer.parseInt(checkOutFields[5]);
		maxcheckouts=Integer.parseInt(checkOutFields[5]);
		
		if (material == null || mincheckouts == null || maxcheckouts== null) {
			return;
		}	
		try {
			mapoutPut.setMinCheck(mincheckouts);
			mapoutPut.setMaxCheck(maxcheckouts);
			context.write(material,mapoutPut);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
      } 
  	 }
	}
  }

  /*Below reducer class is used find min and max checkouts for material type in dataset. Material type is key and from the values for each key, we identify min and max checkouts.*/
  public static class CheckOutsReducer
       extends Reducer<Text,MinMaxCheckOuts,Text,MinMaxCheckOuts> {
	  private MinMaxCheckOuts result = new MinMaxCheckOuts();
	 
	  /*Below reduce function will identify min and max check outs for each key. Where key is Material type in dataset*/
     public void reduce(Text key, Iterable<MinMaxCheckOuts> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Integer mincheckout = 0;
      Integer maxcheckout = 0;
      //initialize min and max values as null for each key.
      result.setMinCheck(null);
      result.setMaxCheck(null);
      
      //Iterate through values of each key and find min and Max
      for (MinMaxCheckOuts val : values) {
    	  
    	  mincheckout = val.getMinCheck();
    	  maxcheckout = val.getMaxCheck();
    	  // get min score 
          
          if (result.getMinCheck()==null || mincheckout.compareTo(result.getMinCheck())<0) {
        	  result.setMinCheck(mincheckout);        
        	  }                                  
          if (result.getMaxCheck()==null || maxcheckout.compareTo(result.getMaxCheck())>0) {
        	  result.setMaxCheck(maxcheckout);
		  }
       }
      
      context.write(key,result);
    }
 
  } 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("year", args[2]);
    Job job = new Job(conf, "MinMax");
    job = Job.getInstance(conf, "CheckOut Count");
    job.setJarByClass(CheckOutsMinMax.class);
    job.setMapperClass(CheckOutsMapper.class);
    job.setCombinerClass(CheckOutsReducer.class);
    job.setReducerClass(CheckOutsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MinMaxCheckOuts.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
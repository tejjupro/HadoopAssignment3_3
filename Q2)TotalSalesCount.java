package SalesTotal;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import SalesTotal.TotalSalesCount;
import SalesTotal.MapperClass;
import SalesTotal.ReducerClass;

public class TotalSalesCount {

			 public static void main(String[] args) throws Exception {
				    if (args.length != 2) {
				      System.err.println("Usage: Product <input path> <output path>");
				      System.exit(-1);
				    }
				    
					//Job Related Configurations
					Configuration conf = new Configuration();
					Job job = new Job(conf, "T0tal units sold in each state for onida");
					job.setJarByClass(TotalSalesCount.class);				    
				    
				    //Provide paths to pick the input file for the job
				    FileInputFormat.setInputPaths(job, new Path(args[0]));
				    
				    
				    //Provide paths to pick the output file for the job, and delete it if already present
					Path outputPath = new Path(args[1]);
					FileOutputFormat.setOutputPath(job, outputPath);
					outputPath.getFileSystem(conf).delete(outputPath, true);

				    
				    //To set the mapper and MapperClass of this job
				    job.setMapperClass(MapperClass.class);
				    job.setReducerClass(ReducerClass.class);
				    
				    //Set the combiner
				    job.setCombinerClass(ReducerClass.class);
				    
				    //set the input and output format class
				    job.setInputFormatClass(TextInputFormat.class);
				    job.setOutputFormatClass(TextOutputFormat.class);

				    //set up the output key and value classes 	
				    job.setOutputKeyClass(Text.class);
				    job.setOutputValueClass(IntWritable.class);
				    
				    //execute the job
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
		 } 
	}




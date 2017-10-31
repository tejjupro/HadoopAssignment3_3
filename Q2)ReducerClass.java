package SalesTotal;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class ReducerClass extends
Reducer<Text, IntWritable, Text, IntWritable> {
public void reduce(Text key, Iterable<IntWritable> valueList,
 Context con) throws IOException, InterruptedException {
try {
	 System.out.println("From The Reducer=>"+key) ;	

     int sum = 0;
     for (IntWritable value : valueList) {
		sum+=value.get();	//Price calculation 
      }		
     // Key is state
      con.write(key, new IntWritable(sum));
	   
} catch (Exception e) {
 e.printStackTrace();
}
}
}

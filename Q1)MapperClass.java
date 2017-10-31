package SalesTotal;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends
Mapper<LongWritable, Text, Text, IntWritable> {
public void map(LongWritable key, Text value, Context con)
 throws IOException, InterruptedException {
	String[] svalue=value.toString().split(Pattern.quote("|"));
	String company = svalue[0].toString(); //Zero Position is Company 
	try {
	   if(!company.contains("NA"))
	   {
			 int price = Integer.parseInt(svalue[5]); // 5th Position is Price
			 con.write(new Text(company), new IntWritable(price));
	   }
	} catch (Exception e) {
	e.printStackTrace();
}
}
}

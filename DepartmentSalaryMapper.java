
import java.io.IOException; 
import java.util.StringTokenizer; 
  
/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project.
 */





import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.FloatWritable;

//The MaxTempMapper has 4 parameters.
//The first two parameters dictate the inputs to the Mapper class. The Key and Value pair
//The third and fourth parameters tells us the output from Mapper class Key and Value 

//First parameter LongWritable = InputKey
//Second parameter Text = InputValue
//Third parameter Intwritable = OutputKey
//Fourth parameter FloatWritable = OutputValue

//First two parameters are - Input Key, Input Value. That is LongWritable and  Text

//Input Key is the line number in the log file. Since we dont care about it, we are just casting as Longwritable.
//Input value is the actual line in the log file, a java string data type, the equivalent MapReduce class is Text.

//Output Key is the IntWriteable and Output value is IntWritable. Mapper outputs are Deparment Id and  Employee Salary 

//The mapper function is quite straightforward. We split the line into tokens. Extract campaign and action fields. Wrap these two as IntWritables and write them out. 
public class DepartmentSalaryMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {	

	 Text k= new Text(); 
	 
	 @Override
    public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
		 
	 
		 String[] tokens = record.toString().split(",");
		 
		 if (tokens.length !=5)
		 {
			 System.out.println("*** invalid record  : " + record);
			 
		 }
		 
		 //first put both of them into string variable
		 String departmentidstr = tokens[3];
		 String salarystr = tokens[4];	 
		 
		 
		 try{
			 
			 //System.out.println("reading first record");
			 
			 System.out.println("deparmentid =" + departmentidstr + "and salary = " + salarystr);			 
			 
			 
			 //int departmentid = Integer.parseInt(departmentidstr.trim());	
			 k.set(departmentidstr);
			 
			 float salary = Float.parseFloat(salarystr.trim());
			 
			 
			 //IntWritable outputKeyFromMapper = new IntWritable(departmentid);
			 FloatWritable outputValueFromMapper = new FloatWritable(salary);
			 
			 context.write(k, outputValueFromMapper);
		 }
		 
		 catch(Exception e){
			 System.out.println("*** there is exception"); 
			 e.printStackTrace(); 
		 }
}
	 
}

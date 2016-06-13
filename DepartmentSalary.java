
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
 
public class DepartmentSalary {
 
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DepartmentSalary <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
 
        //Define MapReduce job
        Job job = new Job(conf, "DepartmentSalary");
        
        job.setJarByClass(DepartmentSalary.class);
        
  
        //Set input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
        //Set Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        //Set Mapper and Reduce classes
        job.setMapperClass(DepartmentSalaryMapper.class);
        job.setReducerClass(DepartmentSalaryReducer.class);
        
        //setting the second argument as a path in a path variable	         
        Path outputPath = new Path(args[1]);
        
        //deleting the output path automatically from hdfs so that we don't have delete it explicitly	         
        outputPath.getFileSystem(conf).delete(outputPath);
 
        //Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
 
        //Submit job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


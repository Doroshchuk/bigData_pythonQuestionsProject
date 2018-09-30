package project;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class PythonQuestionsDriver {
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        if (args.length != 2){
            System.out.println("Usage: project.PythonQuestionsDriver <input dir> <output dir>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

//        PythonQuestionsMapper mapper = new PythonQuestionsMapper();
//        mapper.map(new Text(new String(Files.readAllBytes(Paths.get("E:\\SmallQuestions.csv")))));

        // Create configuration
        //Configuration conf = new Configuration(true);

        // Create job
        Job job = Job.getInstance();
        job.setJarByClass(PythonQuestionsDriver.class);
        job.setJobName("PythonQuestionsDriver");

        // Input
        FileInputFormat.addInputPath(job, inputPath);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);

        // Setup MapReduce
        job.setMapperClass(PythonQuestionsMapper.class);
        job.setReducerClass(PythonQuestionsReducer.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        // Delete output if exists
//        FileSystem hdfs = FileSystem.get(conf);
//        if (hdfs.exists(outputDir))
//            hdfs.delete(outputDir, true);

        //Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

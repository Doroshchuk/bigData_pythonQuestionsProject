package project;

import in.ashwanthkumar.hadoop2.mapreduce.lib.input.CSVLineRecordReader;
import in.ashwanthkumar.hadoop2.mapreduce.lib.input.CSVNLineInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class PythonQuestionsDriver {
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        if (args.length != 2){
            System.out.println("Usage: project.PythonQuestionsDriver <input dir> <output dir>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        Configuration conf = new Configuration();
        conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
        conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
        conf.setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
        conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);

        Job job = Job.getInstance(conf, "PythonQuestionsDriver");
        job.setJarByClass(PythonQuestionsDriver.class);
        job.setMapperClass(PythonQuestionsMapper.class);
        job.setInputFormatClass(CSVNLineInputFormat.class);
        //job.setCombinerClass(PythonQuestionsReducer.class);
        job.setReducerClass(PythonQuestionsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        //Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

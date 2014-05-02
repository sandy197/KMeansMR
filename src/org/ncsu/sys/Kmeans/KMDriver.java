package org.ncsu.sys.Kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMDriver {
	
	//TODO : input/output paths
	private static final String KM_DATA_DIR = "tmp/kmeans/";
	private static final String KM_CENTER_INPUT_PATH = KM_DATA_DIR + "/0";
	private static final String KM_CENTER_OUTPUT_PATH = KM_DATA_DIR + "/0";
//	private static final String KM_INPUT_PATH_1 = KM_DATA_DIR + "/1";
//	private static final String KM_INPUT_PATH_2 = KM_DATA_DIR + "/2";
//	private static final String KM_INPUT_PATH_3 = KM_DATA_DIR + "/3";
//	private static final String KM_INPUT_PATH_4 = KM_DATA_DIR + "/4";
	private static final String KM_TEMP_CLUSTER_DIR_PATH = KM_DATA_DIR + "/tmpC";
	private static final String KM_TEMP_DIR_PATH = KM_DATA_DIR;
	private static final String KM_TEMP_OUTPUT_PATH = KM_DATA_DIR + "/tmp/C";
	private static final String KM_OUTPUT_PATH = KM_DATA_DIR + "/C";
	private static final String MAX_ITERATIONS_KEY = "KM.maxiterations";
	
	private static FileSystem fs;
	private static Configuration conf;

	public static void main(String[] args) throws Exception {
		GenericOptionsParser goParser = new GenericOptionsParser(conf, args);
		fs = FileSystem.get(conf);
		fs.mkdirs(new Path(KM_DATA_DIR));
		KMDriver driver = new KMDriver();
		String[] remainingArgs = goParser.getRemainingArgs();
		
		if (remainingArgs.length < 6) {
		     System.out.println("USAGE: <INPUT_PATH> <OUTPUT_PATH> <COUNT> <K> <DIMENSION OF VECTORS> <MAXITERATIONS> <optional: num of tasks>");
		      return;
		}

		conf = new Configuration();
		int count = Integer.parseInt(remainingArgs[2]);
		int k = Integer.parseInt(remainingArgs[3]);
		int dimension = Integer.parseInt(remainingArgs[4]);
		int iterations = Integer.parseInt(remainingArgs[5]);
		int mapTaskCount = Integer.parseInt(remainingArgs[6]);
		conf.setInt(MAX_ITERATIONS_KEY, iterations);

		Path in = new Path(remainingArgs[0]);
		Path out = new Path(remainingArgs[1]);
		
		//TODO:check if we need this
		Path center = new Path(in, "center/cen.seq");
		Path centerOut = new Path(out, "center/center_output.seq");
		
		String[] inputPath = new String[mapTaskCount];
		for(int i = 0; i < mapTaskCount; i++){
			inputPath[i] = fs.makeQualified(new Path(KM_DATA_DIR + "/" + i)).toString();
			conf.set("KM.inputPath"+i, inputPath[i]);
		}
		
		conf.setInt("KM.mapTaskCount", mapTaskCount);
		conf.set("KM.centerIn", center.toString());
	    conf.set("KM.centerOut", centerOut.toString());
	    
	    String outPath = fs.makeQualified(new Path(KM_OUTPUT_PATH)).toString();
	    String tempDirPath = fs.makeQualified(new Path(KM_TEMP_DIR_PATH)).toString();
	    String tempClusterDirPath = fs.makeQualified(new Path(KM_TEMP_CLUSTER_DIR_PATH)).toString();
	    conf.set("KM.outputDirPath", outPath);
	    conf.set("KM.tempDirPath", tempDirPath);
	    conf.set("KM.tempClusterDir", tempClusterDirPath);
//	    conf.setInt("SpMM.strategy", strategy);
	    conf.setInt("KM.R1", mapTaskCount);
//	    conf.setInt("KM.R2", mapTaskCount);
//	    conf.setInt("SpMM.I", aRows);
//	    conf.setInt("SpMM.K", aColsbRows);
//	    conf.setInt("SpMM.J", bCols);
//	    conf.setInt("SpMM.IB", aRowBlk);
//	    conf.setInt("SpMM.KB", aColbRowBlk);
//	    conf.setInt("SpMM.JB", bColBlk);
	    fs.delete(new Path(tempDirPath), true);
		fs.delete(new Path(outPath), true);
		driver.kmeans();
	}
	
	public void kmeans() throws Exception{
		Job job = Job.getInstance(conf, "kmeans");
		job.setJarByClass(org.ncsu.sys.Kmeans.KMDriver.class);
		
		job.setNumReduceTasks(conf.getInt("KM.R1", 6));
	    System.out.println("Number of reduce tasks for job1 set to: "+ conf.getInt("SpMM.R1", 0));
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
 		job.setMapperClass(KMMapper.class);
 		job.setReducerClass(KMReducer.class);
	    job.setPartitionerClass(KMPartitioner.class);
	    job.setMapOutputKeyClass(org.ncsu.sys.Kmeans.KMTypes.Key.class);
	    job.setMapOutputValueClass(org.ncsu.sys.Kmeans.KMTypes.Value.class);
	    job.setOutputKeyClass(org.ncsu.sys.Kmeans.KMTypes.Key.class);
	    job.setOutputValueClass(org.ncsu.sys.Kmeans.KMTypes.Value.class);
	    
	    FileInputFormat.addInputPath(job, new Path(conf.get("SpMM.inputPathA")));
	    FileInputFormat.addInputPath(job, new Path(conf.get("SpMM.inputPathB")));
	    FileOutputFormat.setOutputPath(job, (new Path(conf.get("SpMM.tempDirPath"))));
	    
	    //TODO: fix all the paths and implement the algo as indicated in the site.
	    
		if (!job.waitForCompletion(true))
			return;
	}

}

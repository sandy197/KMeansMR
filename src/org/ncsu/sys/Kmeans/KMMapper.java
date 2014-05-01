package org.ncsu.sys.Kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.ncsu.sys.Kmeans.KMTypes.Key;
import org.ncsu.sys.Kmeans.KMTypes.Value;
import org.ncsu.sys.Kmeans.KMTypes.VectorType;

public class KMMapper extends Mapper<Key, Value, Key, Value> {
	
	private int dimension;
	private int R1;
	
	public void setup (Context context) {
		init(context);
	}

	private void init(Context context) {
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("KM.dimension", 2);
		R1 = conf.getInt("KM.R1", 4);
	}

	// TODO : set the input path to only files containing 
	// newcentroids from the second iteration
	public void map(Key ikey, Value ivalue, Context context)
			throws IOException, InterruptedException {
		if(ikey.getType() == VectorType.REGULAR)
			context.write(ikey, ivalue);
		else if(ikey.getType() == VectorType.CENTROID){
			//send it to all reduce tasks
			for(int i = 0; i < R1; i++){
				context.write(new Key(i, ikey.getType()), ivalue);
			}
		}
	}
}

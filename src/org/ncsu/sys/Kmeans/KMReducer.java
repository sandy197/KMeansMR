package org.ncsu.sys.Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.ncsu.sys.Kmeans.KMTypes.Key;
import org.ncsu.sys.Kmeans.KMTypes.Value;
import org.ncsu.sys.Kmeans.KMTypes.VectorType;

public class KMReducer extends Reducer<Key, Value, Key, Value> {

	private int dimension;
	private List<int[]> centroids, vectors;
	private int R1;
	
	public void setup (Context context) {
		init(context);
	}
	
	private void init(Context context) {
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("KM.dimension", 2);
		R1 = conf.getInt("KM.R1", 2);
		centroids = new ArrayList<int[]>(R1);
	}

	public void reduce(Key _key, Iterable<Value> values, Context context)
			throws IOException, InterruptedException {
		//populate clusters and data
		if(_key.getType() == VectorType.CENTROID)
			buildCentroids(values, centroids);
		else{
			//TODO : use build and set here
			buildCentroids(values, vectors);
			//buildVectors(values, vectors);
		}
		//TODO : compute the partial clusters
		//TODO: sync
		//TODO: read files and compute newClusters
	}

	private void buildCentroids(Iterable<Value> values, List<int[]> centroidsLoc) {
		for(Value val : values){
			centroidsLoc.add(val.getCoordinates());
		}
		
	}

}

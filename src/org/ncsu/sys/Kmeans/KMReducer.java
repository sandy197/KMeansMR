package org.ncsu.sys.Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.zookeeper.KeeperException;
import org.ncsu.sys.Kmeans.KMTypes.Key;
import org.ncsu.sys.Kmeans.KMTypes.Value;
import org.ncsu.sys.Kmeans.KMTypes.VectorType;
import org.ncsu.sys.Kmeans.SyncPrimitive.Barrier;

public class KMReducer extends Reducer<Key, Value, Key, Value> {

	private static final boolean DEBUG = true;
	public static String ZK_ADDRESS = "10.1.255.13:2181";
	private int dimension;
	private List<Value> centroids, vectors;
	private int R1;
	boolean isCbuilt, isVbuilt;
	
	public void setup (Context context) {
		init(context);
	}
	
	private void init(Context context) {
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("KM.dimension", 2);
		R1 = conf.getInt("KM.R1", 2);
		centroids = new ArrayList<Value>(R1);
		isCbuilt = isVbuilt = false;
	}

	public void reduce(Key _key, Iterable<Value> values, Context context)
			throws IOException, InterruptedException {
		//populate clusters and data
		Value[] partialCentroids = null;
		
		if(_key.getType() == VectorType.CENTROID){
			buildCentroids(values, centroids);
			isCbuilt = true;
		}
		else{
			//TODO : use build and set here
			buildCentroids(values, vectors);
			isVbuilt = true;
			//buildVectors(values, vectors);
		}
		//TODO : compute the partial clusters
		if(isCbuilt && isVbuilt){
			try{
				partialCentroids = classify(vectors, centroids);
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
			
			//TODO: check the case where a cluster doesn't contain any points. Refer to the fix pointed out on the site.
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			int taskId = context.getTaskAttemptID().getTaskID().getId();
			//TODO: write the partial centroids to files
			Path path = new Path(conf.get("KM.tempClusterDir" + "/" + taskId));
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
				      IntWritable.class, Value.class,
				      SequenceFile.CompressionType.NONE);
			for(int i = 0; i < partialCentroids.length ; i++){
				Value centroid = (Value)centroids.get(i);
				IntWritable el = new IntWritable();
				el.set(partialCentroids[i].getCentroidIdx());
				writer.append(el, partialCentroids[i]);
			}
			writer.close();
			//sync
			Barrier b = new Barrier(ZK_ADDRESS, "/b1", R1);
			try{
			    boolean flag = b.enter();
			    System.out.println("Entered barrier: " + 6);
			if(!flag) System.out.println("Error when entering the barrier");
			} catch (KeeperException e){
				e.printStackTrace();
			} catch (InterruptedException e){
				e.printStackTrace();
			}
			
			//TODO: read files and compute newClusters only if its the task 0
			if(taskId == 0){
				//TODO : add partial centers of task 0 to the hashmap.
				Hashtable<Integer, Value> auxCentroids = new Hashtable<Integer, Value>();
				for(int i = 0; i < partialCentroids.length; i++){
					auxCentroids.put(partialCentroids[i].getCentroidIdx(), partialCentroids[i]);
				}
				
				//add partial centers of other tasks to the hashmap
				for(int i = 1; i < R1; i++){
					//configureWithClusterInfo for reading a file
					path = new Path(conf.get("KM.tempClusterDir") + "/" + R1);
					Path filePath = fs.makeQualified(path);
					List<Value> partCentroidsFromFile = getCentroidsFromFile(filePath);
					for(Value partialCentroid : partCentroidsFromFile){
						if(auxCentroids.containsKey(partialCentroid.getCentroidIdx())){
							//TODO: clarify changes to count and consider corner cases
							auxCentroids.get(partialCentroid.getCentroidIdx()).addVector(partialCentroid);
						}
						else{
							auxCentroids.put(partialCentroid.getCentroidIdx(), partialCentroid);
						}
					}
				}
				
				int centroidCount = auxCentroids.keySet().size();
				for(Integer key : auxCentroids.keySet()){
					//TODO: compute new clusters
					Value newCentroid = computeNewCentroid(auxCentroids.get(key));
					//TODO:write it back as a standard reducer output !
					if(newCentroid != null)
						context.write(new Key(R1, VectorType.CENTROID), newCentroid);
				}
			}
		}
	}

	private Value computeNewCentroid(Value value) {
		if(value.getCount() == 0)
			return null;
		else {
			Value newCentroid = new Value(value.getDimension());
			int[] coords = value.getCoordinates();
			int[] newCoords = newCentroid.getCoordinates();
			for(int i = 0; i < coords.length; i++){
				newCoords[i] = coords[i]/value.getCount();
			}
			return newCentroid;
		}
			
	}

	private List<Value> getCentroidsFromFile(Path filePath) {
		List<Value> partialCentroids = new ArrayList<Value>();
		Configuration conf = new Configuration();
		Reader reader = null;
		try {
			FileSystem fs = filePath.getFileSystem(conf);
			reader = new SequenceFile.Reader(fs, filePath, conf);
			Class<?> valueClass = reader.getValueClass();
			IntWritable key;
			try {
				key = reader.getKeyClass().asSubclass(IntWritable.class).newInstance();
			} catch (InstantiationException e) { // Should not be possible
				throw new IllegalStateException(e);
			} catch (IllegalAccessException e) {
					throw new IllegalStateException(e);
			}
			Value value = new Value();
			while (reader.next(key, value)) {
				partialCentroids.add(value);
				value = new Value();
			}
        } catch (IOException e) {
			e.printStackTrace();
		} finally {
        	try{
        		reader.close();
        	} catch (IOException e) {
        		e.printStackTrace();
        	}
        }
		return partialCentroids;
	}

	private Value[] classify(List<Value> vectors2, List<Value> centroids2) throws Exception {
		Value[] partialCentroids = new Value[centroids2.size()];
		for(Value point : vectors2){
			int idx = getNearestCentroidIndex(point, centroids2);
			if(partialCentroids[idx] == null){
				partialCentroids[idx] = new Value(point.getDimension());
			}
			partialCentroids[idx].addVector(point);
			if(partialCentroids[idx].getCentroidIdx() == -1){
				partialCentroids[idx].setCentroidIdx(idx);
			}
			else {
				if(partialCentroids[idx].getCentroidIdx() != idx)
					if(DEBUG) throw new Exception("Fatal: Inconsistent cluster, multiple centroids problem!");
			}
		}
		return partialCentroids;
	}

	private int getNearestCentroidIndex(Value point, List<Value> centroids2) {
		int nearestCidx = -1;
		int shortestDistance = Integer.MAX_VALUE;
		for(Value centroid : centroids2){
			int distance = getDistance(point.getCoordinates(), centroid.getCoordinates());
			if(distance < shortestDistance){
				nearestCidx  = centroid.getCentroidIdx();
				shortestDistance = distance;
			}
		}
		return nearestCidx;
		
	}

	private int getDistance(int[] point, int[] centroid) {
		int distance = 0;
		for(int i = 0; i < point.length; i++){
			distance += (point[i] - centroid[i]) * (point[i] - centroid[i]); 
		}
		return distance;
	}

	private void buildCentroids(Iterable<Value> values, List<Value> centroidsLoc) {
		for(Value val : values){
			centroidsLoc.add(val);
		}
		
	}

}

package org.ncsu.sys.Kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KMTypes {
	
	public static enum VectorType{
		REGULAR(0), CENTROID(1);
		
		private int typeVal;
		private VectorType(int typeVal){
			this.typeVal = typeVal;
		}
		
		public int getTypeVal(){
			return this.typeVal;
		}

		public static VectorType getType(int readInt) {
			switch(readInt){
			case 0:
				return REGULAR;
			case 1:
				return CENTROID;
			default:
				return REGULAR;
			}
		}
	}
	
	public static class Value implements WritableComparable{
		
		private int dimension;
		private int[] coordinates;
		private int count;
		private int centroidIdx;
		
		public Value(int dimension){
			this.dimension = dimension; 
			this.coordinates = new int[dimension];
			for(int i = 0; i < dimension; i++)
				this.coordinates[i] = 0;
			this.count = 0;
			centroidIdx = 1;
			
		}
		
		public int getCentroidIdx() {
			return centroidIdx;
		}

		public void setCentroidIdx(int centroidIdx) {
			this.centroidIdx = centroidIdx;
		}
		
		public int getDimension() {
			return dimension;
		}
		
		public void setDimension(int dimension) {
			this.dimension = dimension;
		}
		
		public int[] getCoordinates() {
			return coordinates;
		}

		public void setCoordinates(int[] coordinates) {
			this.coordinates = coordinates;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			dimension = in.readInt();
			for(int i = 0; i < dimension; i++){
				in.readInt();
			}
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(dimension);
			if(coordinates != null){
				for(int i = 0; i < dimension; i++){
					out.writeInt(coordinates[i]);
				}
			}
			out.writeInt(count);
		}
		@Override
		public int compareTo(Object o) {
			Value val = (Value)o;
			int[] oCoords = val.getCoordinates();
			for(int i =0; i < dimension; i++){
				if(this.coordinates[i] < oCoords[i]){
					return -1;
				}
				else if(this.coordinates[i] > oCoords[i]){
					return 1;
				}
			}
			return 0;
		}

		public void addVector(int[] vector) {
			for(int i = 0; i < this.dimension; i++){
				this.coordinates[i] += vector[i];
			}
			this.count++;
		}
		
		public void addVector(Value vector){
			int[] coords = vector.getCoordinates();
			for(int i = 0; i < this.dimension; i++){
				this.coordinates[i] += coords[i];
			}
			this.count += vector.getCount();
		}
		
	}
	
	
	public static class Key implements WritableComparable{

		private int TaskIndex;
		private VectorType type;
		
		//This is assigned by the map task
		private int centroidIdx;

		public Key(int TaskIndex, VectorType type) {
			this.TaskIndex = TaskIndex;
			this.type = type;
		//	this.centroidIdx = -1;
		}

		public VectorType getType() {
			return type;
		}

		public void setType(VectorType type) {
			this.type = type;
		}

		public int getTaskIndex() {
			return TaskIndex;
		}

		public void setTaskIndex(int taskIndex) {
			TaskIndex = taskIndex;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			TaskIndex = in.readInt();
			type = VectorType.getType(in.readInt());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(this.TaskIndex);
			out.writeInt(type.getTypeVal());
		}

		@Override
		public int compareTo(Object o) {
			Key key = (Key)o;
			if(this.TaskIndex < key.getTaskIndex())
				return -1;
			else if(this.TaskIndex > key.getTaskIndex())
				return 1;
			//CENTROID types are greater than REGULAR types
			else if(this.getType().getTypeVal() < key.getType().getTypeVal())
				return -1;
			else if(this.getType().getTypeVal() < key.getType().getTypeVal())
				return 1;
			else
				return 0;
		}
	}
}

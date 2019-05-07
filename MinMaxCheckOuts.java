import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/*Below MinMaxCheckOuts class is used to hold both min and max values for each key in result*/
public class MinMaxCheckOuts implements Writable {
	// variables declaration
	Integer minCheckouts;
	Integer maxCheckouts;
	// constructor
	public MinMaxCheckOuts() {
		minCheckouts=0;
		maxCheckouts=0;
	}
	//set method
	void setMinCheck(Integer checkout){
		this.minCheckouts=checkout;
	}
	void setMaxCheck(Integer checkout){
		this.maxCheckouts=checkout;
	}
	//get method
	Integer getMinCheck() {
		return minCheckouts;
	}
	Integer getMaxCheck(){
		return maxCheckouts;
	}
	
	// write method
			public void write(DataOutput out) throws IOException {
			// Order for writing
				out.writeInt(minCheckouts);
				out.writeInt(maxCheckouts);
		}
		 
		 // readFields Method
		public void readFields(DataInput in) throws IOException {
			minCheckouts=new Integer(in.readInt());
			maxCheckouts=new Integer(in.readInt());
		}

		public String toString() {
			return minCheckouts + "\t" + maxCheckouts;
		}

}
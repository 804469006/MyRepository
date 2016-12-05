package toblejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CustomerOrderBean implements Writable{
	private String fiag;
	private String data;

	public void readFields(DataInput in) throws IOException {
		this.fiag=in.readUTF();
		this.data=in.readUTF();
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(fiag);
		out.writeUTF(data);
		
	}
    
	
	public CustomerOrderBean() {
		
	}

	public void set(String fiag, String data) {
		this.fiag = fiag;
		this.data = data;
	}

	public String getFiag() {
		return fiag;
	}

	public void setFiag(String fiag) {
		this.fiag = fiag;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return  fiag + ", " + data ;
	}
	

}

package com.ibeifeng.hadoop.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Traffic implements WritableComparable<Traffic> {
	private int upPackNum;
	private int downPackNum;
	private int upPayLoad;
	private int downPayLoad;

	public Traffic() {
	}

	public void set(int upPackNum, int downPackNum, int upPayLoad,
			int downPayLoad) {
		this.upPackNum = upPackNum;
		this.downPackNum = downPackNum;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
	}

	// 反序列化
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readInt();
		this.downPackNum = in.readInt();
		this.upPayLoad = in.readInt();
		this.downPayLoad = in.readInt();

	}

	// 序列号
	public void write(DataOutput out) throws IOException {
		out.writeInt(upPackNum);
		out.writeInt(downPackNum);
		out.writeInt(upPayLoad);
		out.writeInt(downPayLoad);
	}

	public int compareTo(Traffic o) {
		int num = (o.getUpPayLoad()+o.getDownPayLoad())-(this.getUpPayLoad()+this.getDownPayLoad());
		return num;
	}

	public int getUpPackNum() {
		return upPackNum;
	}

	public void setUpPackNum(int upPackNum) {
		this.upPackNum = upPackNum;
	}

	public int getDownPackNum() {
		return downPackNum;
	}

	public void setDownPackNum(int downPackNum) {
		this.downPackNum = downPackNum;
	}

	public int getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(int upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public int getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(int downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	@Override
	public String toString() {
		return "TotalPackNum=" + (upPackNum + downPackNum) + "\t"
				+ "TotalPayLoad=" + (upPayLoad + downPayLoad);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + downPackNum;
		result = prime * result + downPayLoad;
		result = prime * result + upPackNum;
		result = prime * result + upPayLoad;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Traffic other = (Traffic) obj;
		if (downPackNum != other.downPackNum)
			return false;
		if (downPayLoad != other.downPayLoad)
			return false;
		if (upPackNum != other.upPackNum)
			return false;
		if (upPayLoad != other.upPayLoad)
			return false;
		return true;
	}
    
}

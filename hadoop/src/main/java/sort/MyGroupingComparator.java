package sort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupingComparator implements RawComparator<KeyBean>{
	//并未实际使用到
	public int compare(KeyBean o1, KeyBean o2) {
		System.out.println("compare...");
		return o2.getLeft().compareTo(o1.getLeft());
	}
	/**
	 * 	b1:key1的字节表现
	 * 	s1:index
	 * 	l1:length
	 *	以字节的形式进行比较两个key
	 *	目的是将key:a#1  a#3  a#100均划分为一个group中得到a#1:{1,3,100}
	 *	所以最后一位不能纳入比较,由于一个int是4个字节,故-4
	 */
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		System.out.println(b1);
		System.out.println(b2);
		return WritableComparator.compareBytes(b1, 0, l1-4, b2, 0, l2-4);
	}

}

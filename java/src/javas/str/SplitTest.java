package javas.str;

import java.util.ArrayList;

public class SplitTest {
        public static void main(String[] args) {
			
		}
        public static String[] mySplit(String str){
        	ArrayList<String> arrayList = new ArrayList<String>();
        	
        	int len = -1;
        	while((len = str.indexOf(',')) != -1){
        		arrayList.add(str.substring(0, len));
        		str = str.substring(len + 1);
        	}
        	arrayList.add(str);
			return arrayList.toArray(new String[arrayList.size()]);
        }
}

package javas.str;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOTest {
	public static void main(String[] args) {
		FileInputStream fin =null;
		FileOutputStream fos =null;
		FileChannel fic=null;
		FileChannel foc=null;
		try {
			fin = new FileInputStream("d:/11.sql");
			fos = new FileOutputStream("e:/11.sql");
		    fic= fin.getChannel();
		    foc = fos.getChannel();
		    
		    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
		    int len = -1 ;
		    while((len = fic.read(byteBuffer))!=-1){
		    	byteBuffer.flip();
		    	foc.write(byteBuffer);
		    	byteBuffer.clear();
		    }
		} catch (Exception e) {			
			e.printStackTrace();
		}finally{
			try {
				if(foc != null){
					foc.close();
				}
				if(fic != null){
					fic.close();
				}
				if(fos != null){
					fos.close();
				}
				if(fin != null){
					fin.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}

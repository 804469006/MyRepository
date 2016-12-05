package javas.str;

public class SingleTon2 {
	//¶öººÊ½
	private SingleTon2(){}
	
	private static SingleTon2 instance = new SingleTon2();
	
	public static SingleTon2 getInstance(){
		
		return instance;
	}
}

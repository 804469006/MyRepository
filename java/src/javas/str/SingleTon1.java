package javas.str;

public class SingleTon1 {
	//¿¡∫∫ Ω
	private SingleTon1(){}
	
	private static SingleTon1 instance = null;
	//***********************************
	
	public static SingleTon1 getInstance(){
		
		if (instance==null){
			
			synchronized (SingleTon1.class) {
				
				if(instance==null){
					instance = new SingleTon1();
				}
			}
		}
		return instance;
	}

}

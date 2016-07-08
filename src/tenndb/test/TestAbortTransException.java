package tenndb.test;

import tenndb.tx.AbortTransException;

public class TestAbortTransException {

	public static void test() throws AbortTransException{
		try{
			System.out.println("test.1");
			throw new AbortTransException("abort ");
			
		}catch(AbortTransException ae){
			System.out.println(ae);
			System.out.println("test.2");
			throw ae;
		}
		catch(Exception e){
			System.out.println(e);
			System.out.println("test.3");
		}
		finally{
			System.out.println("test.4");
		}
	}
	
	
	public static void main(String[] args) {
		
		try {
			test();
		} catch (AbortTransException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

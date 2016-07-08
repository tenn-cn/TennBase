package tenndb.hash.test;

import tenndb.hash.CRC32HashProvider;

public class TestCRC {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		CRC32HashProvider provider = new CRC32HashProvider();
		String[] array = new String[10000];
		
		long t1 = System.currentTimeMillis();		
		for(int i = 0; i < 10000; ++i)
		{
			array[i] = "helloworld_" + i;
		}
		
		long t2 = System.currentTimeMillis();
		for(int i = 0; i < 10000; ++i)
		{
			provider.hash(array[i]);
		}
		long t3 = System.currentTimeMillis();
		
		System.out.println((t3 - t1) + ", " + (t2 - t1) + ", " + (t3 - t2));
	}

}

package tenndb.disttest;

import java.util.Date;

public class TestFileName {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		for(int i = 0; i < 100; ++i){
			long time = new Date().getTime();
			time %= 1000;
			System.out.println(time + "," + String.format("%04X", time));

		}
	}

}

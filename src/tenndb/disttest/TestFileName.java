package tenndb.disttest;

import java.io.File;
import java.util.Date;

public class TestFileName {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String str = "1607140000";
		System.out.println(str.getBytes().length);
		
/*		for(int i = 0; i < 100; ++i){
			long time = new Date().getTime();
			time %= 1000;
			System.out.println(time + "," + String.format("%04X", time));

		}*/
		
		File dir = new File("J:\\tennbase\\dist_data");
		
		String[] files = dir.list();
		
		if(null != files){
			for(String file : files){
				System.out.println(file);
			}
		}
	}

}

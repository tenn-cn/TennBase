package tenndb.common.test;

import java.util.Date;

import tenndb.common.SystemTime;

public class TestSystemTime {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		for(int i = 0; i < 100; ++i){
			SystemTime sys = SystemTime.getSystemTime();
			System.out.println(new Date().toString() + " " + sys.currentTimeMillis());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}

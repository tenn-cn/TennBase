package tenndb.test;


import java.io.IOException;

import tenndb.common.FileMgr;

public class TestFileMgr {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//snapshot_index_stu
		FileMgr mgr = new FileMgr("J:\\tennbase");
		byte[] buffer = new byte[4];
		for(int i = 0 ; i < 4; ++i)
			buffer[i] = (byte) i;
		try{
			mgr.writeBuffer("snapshot_index_stu", buffer, 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			for(int i = 0 ; i < 4; ++i)
				buffer[i] = (byte) 0;
			mgr.readBuffer("snapshot_index_stu", buffer, 0);

			for(int i = 0 ; i < 4; ++i)
				System.out.println("readBuffer " + i + ", " + buffer[i]);
			

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

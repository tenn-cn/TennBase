package tenndb.test;

import tenndb.common.ByteUtil;

public class TestByte {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		byte[] array = ByteUtil.intToByte4_big(123456);
		
		int n = ByteUtil.byte4ToInt_big(array, 0);
		System.out.println("n = " + n);
	}

}

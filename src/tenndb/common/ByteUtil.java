package tenndb.common;

public final class ByteUtil {

	public static final int INT_SIZE   = Integer.SIZE / Byte.SIZE;
	public static final int SHORT_SIZE = Short.SIZE   / Byte.SIZE;
	
	public final static int SHORT_MAX_VALUE = 65535;
	
	public final static int byte2ToShort_big(byte[] buf, int offset)
	{
		int n = 0;
		if(null != buf)
		{
			for(int i = 0 ;i < 2; i++)
			{
			    n <<= 8;
			    n |= (buf[offset + i] & 0x000000FF);
			}
		}
		return n;
	}
	
	public static byte[] shortToByte2_big(int n)
	{
		byte[] buf = new byte[2];
		
		for(int i = 1; i >= 0; i--){
			buf[i] = (byte) (n & 0xFF);
			n >>= 8;
		}
		
		return buf;
	}
	
	
	public final static int byte4ToInt_big(byte[] buf, int offset)
	{
		int n = 0;
		if(null != buf)
		{
			for(int i = 0 ;i < 4; i++)
			{
			    n <<= 8;
			    n |= (buf[offset + i] & 0x000000FF);
			}
		}
		return n;
	}
	
	public static byte[] intToByte4_big(int n)
	{
		byte[] buf = new byte[4];
		
		for(int i = 3; i >= 0; i--){
			buf[i] = (byte) (n & 0xFF);
			n >>= 8;
		}
		
		return buf;
	}
}

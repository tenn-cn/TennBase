package tenndb.common;


import java.nio.channels.FileChannel;

public class FileDeco {

	protected String fileName;
	protected int ref = 0;
	protected int state;
	protected int size;
	protected FileChannel fc;
	
//	protected MappedByteBuffer out;  
	
	protected final Object lock = new Object();
	
	public static final int STATE_INIT    = 0;
	public static final int STATE_OPEN    = 1;
	public static final int STATE_CLOSING = 2;
	public static final int STATE_CLOSED  = 3;
	
	public FileChannel getFileChannel(){
		return this.fc;
	}
	
	public int incRef(){
		int n = -1;
		synchronized(lock){
			ref++;
			n = ref;
		}
		return n;
	}
	
	public int decRef(){
		int n = -1;
		synchronized(lock){
			ref--;
			n = ref;
		}
		return n;
	}
}

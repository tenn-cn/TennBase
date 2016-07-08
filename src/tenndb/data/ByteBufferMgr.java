package tenndb.data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class ByteBufferMgr {

	protected List<ByteBuffer> list = null;
	
	protected Queue<ByteBuffer> queue = null;

	protected int size;
	
	protected final Object lock = new Object();
		
	public ByteBufferMgr(int size) {
		super();
		this.size = size;
		this.queue = new LinkedBlockingQueue<ByteBuffer>();
		this.list = new ArrayList<ByteBuffer>();
	}
	
	public ByteBuffer pinBuffer(){
		ByteBuffer buffer = null;
		
		buffer = this.queue.poll();
		
		if(null == buffer){
			buffer = ByteBuffer.allocate(this.size);
			this.list.add(buffer);
		}
		
		return buffer;
	}
	
	public void unpinBuffer(ByteBuffer buffer){
		
		if(null != buffer){
			this.queue.add(buffer);
		}
	}
}

package tenndb.dist;


import java.nio.ByteBuffer;


public class DistPage {
//
//	public static final int PAGE_SIZE = 1024 * 1000 * 10;
	
	public static final int PAGE_SIZE = 1024 * 40;
	
	protected ByteBuffer buffer;
	
	protected int limit;
	
	protected int offset;

	protected final Object lock = new Object();
	
	public DistPage(ByteBuffer buffer) {
		super();
		this.buffer  = buffer;
		this.limit   = PAGE_SIZE;
		this.offset  = 0;
	}
	
	public ByteBuffer getBuffer() {
		return buffer;
	}

	public void clear(){
		this.offset = 0;
		this.buffer.clear();
	}
	
	public boolean write(byte[] buff){
		boolean b = false;
		
		if(null != buff && buff.length > 0){
			synchronized(this.lock){
				if(this.offset + buff.length < this.limit){
					this.buffer.position(this.offset);
					this.buffer.put(buff);
					this.offset += buff.length;
					b = true;
				}
			}
		}
		
		return b;
	}

}

package tenndb.dist;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.nio.ByteBuffer;

import tenndb.common.ByteUtil;
import tenndb.common.FileMgr;
import tenndb.thread.DistThread;

public class DistMgr {

	protected FileMgr fileMgr = null;
	
	protected Queue<DistPage> unusedQueue = null;
	
	protected List<DistPage> usedList = null;

	protected AtomicInteger counter = null;
	
	protected static final int MAX_PAGE_NUM = 3;
	
	protected static final int MAX_QUEUE_SIZE = ByteUtil.SHORT_MAX_VALUE - ByteUtil.BYTE_MAX_VALUE;
	
	public static final String DIST_PATH    = System.getProperty("file.separator") + "dist_data";
	
	public static final String PREFIX_TEMP    = "temp_";
	
	public static final String PREFIX_DELETE  = "delete_";
	
	public static final String PREFIX_QUEUE   = "queue_";
	
	public static final byte TAG_DATA = 1;
	public static final byte TAG_NULL = 0;
	
	protected DistThread distThread = null;
	
	protected final ReadWriteLock lock = new ReentrantReadWriteLock(false);
	
    protected volatile boolean initialized = false;
    
	
	public DistMgr(String root){

		this.fileMgr 	 = new FileMgr(root + DIST_PATH);		
		this.unusedQueue = new LinkedBlockingDeque<DistPage>();
		this.usedList    = new ArrayList<DistPage>();
		
		this.counter     = new AtomicInteger(1);
	}
	
	public void init(){
		if(false == this.initialized){
			try{
				this.lockWrite();
				if(false == this.initialized){
					this.distThread  = new DistThread(this);
					this.distThread.start();
					this.initialized = true;
				}
			}catch(Exception e){
				System.out.println(e);
			}finally{
				this.unLockWrite();
			}
		}
	}
	
	public void flush(DistPage page, String fileName){
		if(null != page && null != page.buffer && page.buffer.limit() > 0){
			page.buffer.rewind();
			String tempFileName = PREFIX_TEMP  + PREFIX_QUEUE + fileName;
			String  newfileName = PREFIX_QUEUE + fileName;
			
			this.fileMgr.append(tempFileName, page.buffer);
			this.fileMgr.rename(tempFileName, newfileName);
			this.fileMgr.closeFileChannel(newfileName);
			page.clear();
			
			synchronized(this.lock){
				if(this.unusedQueue.size() < MAX_PAGE_NUM){
					this.unusedQueue.add(page);	
				}				
			}
		}
	}
	
	public String newFileName(){
		String fileName = null;
		int index = this.counter.getAndIncrement();		
		if(index > MAX_QUEUE_SIZE){
			index = this.counter.getAndSet(1);
		}
		fileName = String.format("%04X", index);
		return fileName;
	}

	
	public List<DistPage> getAndClearUsedList(){
		List<DistPage> list = null;
		synchronized(this.lock){
			if(this.usedList.size() > 0){
				list = this.usedList;
				this.usedList = new ArrayList<DistPage>();
			}
		}
		return list;
	}
	
	protected DistPage pinDistPage(){
		DistPage page = this.unusedQueue.peek();
		
		if(null == page){
			ByteBuffer buff = ByteBuffer.allocate(DistPage.PAGE_SIZE);
			page = new DistPage(buff);
			this.unusedQueue.add(page);
		}
		
		return page;
	}
	
	public void write(List<byte[]> list){
		if(null != list && list.size() > 0){
			synchronized(this.lock){
				for(byte[] array : list){
	
					DistPage page = this.pinDistPage();
					boolean b = page.write(array);
					if(false == b){
						this.usedList.add(page);
						this.unusedQueue.poll();
						page = this.pinDistPage();
						b = page.write(array);
					}
				}
			}			
		}
	}
	
	protected void lockRead()    { this.lock.readLock().lock();    }
	
	protected void unLockRead()  { this.lock.readLock().unlock();  }

	protected void lockWrite()   { this.lock.writeLock().lock();   }
	
	protected void unLockWrite() { this.lock.writeLock().unlock(); }
}

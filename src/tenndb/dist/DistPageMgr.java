package tenndb.dist;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;

import tenndb.common.FileMgr;
import tenndb.common.SystemTime;
import tenndb.data.Colunm;

public class DistPageMgr {

	protected FileMgr fileMgr = null;
	
	protected Queue<DistPage> unusedQueue = null;
	
	protected List<DistPage> usedList = null;

	protected final Object lock = new Object();
	
	protected AtomicInteger counter = null;
	
	protected static final int MAX_PAGE_NUM = 3;
	
	public static final String PREFIX_TEMP = "temp_";
	
	public static final String PREFIX_QUEUE = "queue_";
		
	public DistPageMgr(FileMgr fileMgr){

		this.fileMgr 	 = fileMgr;		
		this.unusedQueue = new LinkedBlockingDeque<DistPage>();
		this.usedList    = new ArrayList<DistPage>();
		int time         = SystemTime.getSystemTime().currentTime();
		time            %= 3600*24;
		
		this.counter     = new AtomicInteger(time);
		

	}
	
	public void flush(DistPage page, String fileName){
		if(null != page && null != page.buffer && page.buffer.limit() > 0){
			page.buffer.rewind();
			String tempFileName = PREFIX_TEMP + PREFIX_QUEUE + fileName;
			String  newfileName = PREFIX_QUEUE + fileName;
			
			this.fileMgr.append(tempFileName, page.buffer);
			this.fileMgr.rename(tempFileName, fileName);
			this.fileMgr.closeFileChannel(fileName);
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
	
	public void write(String devID, List<Colunm> colunms){
		if(null != devID && devID.length() > 0 && null != colunms && colunms.size() > 0){
			List<byte[]> list = new ArrayList<byte[]>();
			for(Colunm colunm : colunms){
				byte[] buff = null;
				list.add(buff);
			}
			
			this.write(list);
		}
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
	
}

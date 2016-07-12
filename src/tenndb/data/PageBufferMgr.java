package tenndb.data;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

import tenndb.common.FileDeco;
import tenndb.common.FileMgr;

public class PageBufferMgr {

	protected String dbName = null;
	
	protected Map<Integer, PageBuffer> unusedMap = null;
	
	protected Queue<PageBuffer> unusedQueue = null;
	
	protected List<PageBuffer> usedList = null;
	
	protected ConcurrentMap<Integer, PageBuffer> pageMap = null;

	protected FileMgr fileMgr = null;
	
	protected ByteBufferMgr bufMgr = null;
	
	protected final Object lock = new Object();
	
	protected static final String PREFIX_UNUSED_PAGE = "unused_page_";
	
	protected static final String PREFIX_DATA = "data_";
	
	public PageBufferMgr(String dbName, FileMgr fileMgr){
		this.dbName      = dbName;
		this.fileMgr     = fileMgr;
		
		this.unusedQueue = new LinkedBlockingDeque<PageBuffer>();
		this.usedList    = new ArrayList<PageBuffer>();
		this.pageMap     = new ConcurrentHashMap<Integer, PageBuffer>();
		this.unusedMap   = new HashMap<Integer, PageBuffer>();
		this.bufMgr      = new ByteBufferMgr(DBPage.PAGE_SIZE);
	}
	
	protected DBBlock getBlock(Colunm colunm){
		DBBlock blk = null;
		PageBuffer page = this.unusedQueue.peek();

		if(null != page){

			if(page.isfull(colunm)){	
	
				PageBuffer full = this.unusedQueue.poll();
				this.usedList.add(full);
				this.unusedMap.remove(page.pageID);
				page = this.unusedQueue.peek();
			}else{

			}
			
			if(null != page){
				blk = page.nextBlock(colunm);
			}
		}
		return blk;
	}
	
	public synchronized DBBlock nextBlock(Colunm colunm){
		DBBlock blk = null;
		blk = this.getBlock(colunm);

		if(null == blk){
			this.apppend();
			blk = this.getBlock(colunm);
		}
		
		return blk;
	}
	
	public void flushData(){
		PageBuffer page = this.unusedQueue.peek();
		if(null != page){
			this.setPageBuffer(page);
		}
		
		if(usedList.size() > 0){
			for(int i = 0; i < usedList.size(); ++i){
				PageBuffer buffer = usedList.get(i);
				this.setPageBuffer(buffer);
			}
		}
	}
	
	public void flushPage(){
		
		List<PageUnusedIndex> unusedList = new ArrayList<PageUnusedIndex>();
		if(this.unusedMap.size() > 0){
			
			for(Integer pageID : this.unusedMap.keySet()){
				
				PageBuffer buffer = this.unusedMap.get(pageID);
				
				PageUnusedIndex index = new PageUnusedIndex();
				index.pageID = buffer.pageID;
				index.offset = buffer.offset;
				unusedList.add(index);
			}
		}

		if(null != unusedList && unusedList.size() > 0){
			FileDeco fd = null;
			try {
				fd = this.fileMgr.pinFileChannel(PREFIX_UNUSED_PAGE + this.dbName);
				if(null != fd){
					fd.getFileChannel().position(0);

					ByteBuffer buffer = ByteBuffer.allocate(DBPage.PAGE_INDEX_SIZE * unusedList.size());		
					buffer.rewind();
					for(int i = 0; i < unusedList.size(); ++i){
						PageUnusedIndex index = unusedList.get(i);
						buffer.putInt(index.pageID);
						buffer.putInt(index.offset);
					}
					
					buffer.flip();
					fd.getFileChannel().position(0);

					while(buffer.hasRemaining()) {
						fd.getFileChannel().write(buffer);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				if(null != fd){
					this.fileMgr.closeFileChannel(fd);
				}
			}
		}
	}
	
	public void load(){
		
		List<PageUnusedIndex> unusedList = new ArrayList<PageUnusedIndex>();
		
		FileDeco fd = null;
		try {
			fd = this.fileMgr.pinFileChannel(PREFIX_UNUSED_PAGE + this.dbName);
			if(null != fd){
				int len = (int) fd.getFileChannel().size();
				ByteBuffer buffer = ByteBuffer.allocate(len);				
				int size = fd.getFileChannel().read(buffer);
				int limit = size / DBPage.PAGE_INDEX_SIZE;
				for(int i = 0; i < limit; ++i){
					int pageID = buffer.getInt(i * DBPage.PAGE_INDEX_SIZE);
					int offset = buffer.getInt(i * DBPage.PAGE_INDEX_SIZE + 4);
					
					PageUnusedIndex blk = new PageUnusedIndex();
					blk.pageID = pageID;
					blk.offset = offset;
					
					if(!this.unusedMap.containsKey(blk.pageID)){
						unusedList.add(blk);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(null != fd){
				this.fileMgr.closeFileChannel(fd);
			}
		}
		
		if(null != unusedList && unusedList.size() > 0){
			for(int i = 0; i < unusedList.size(); ++i){
				PageUnusedIndex index = unusedList.get(i);
				PageBuffer buff = this.getPageBuffer(index.pageID);
				if(null != buff){
					buff.offset = index.offset;
					this.unusedQueue.add(buff);
					this.unusedMap.put(buff.pageID, buff);
				}
			}
		}
	}
	
	public void setPageBuffer(PageBuffer buffer){
		if(null != buffer){
			FileDeco fd = null;
			try {
				fd = this.fileMgr.pinFileChannel(PREFIX_DATA + this.dbName);

				if (null != fd && null != fd.getFileChannel() && fd.getFileChannel().isOpen()) {
					int limit = (int) (fd.getFileChannel().size()/DBPage.PAGE_SIZE);
					
					if (buffer.pageID < limit) {
						buffer.buffer.flip();
						int read = fd.getFileChannel().write(buffer.buffer, buffer.pageID * DBPage.PAGE_SIZE);
						if (DBPage.PAGE_SIZE == read) {

						}
					}
				}
			} catch (IOException e) {
				System.out.println(e);
				if (null != fd) {
					this.fileMgr.closeFileChannel(fd);
				}
			}
		}
	}
	
	public PageBuffer getPageBuffer(int pageID) {
		PageBuffer buffer = null;

		buffer = this.pin(pageID);

		if (null == buffer) {

			FileDeco fd = null;
			try {
				fd = this.fileMgr.pinFileChannel(PREFIX_DATA + this.dbName);

				if (null != fd && null != fd.getFileChannel() && fd.getFileChannel().isOpen()) {
					int limit = (int) (fd.getFileChannel().size()/DBPage.PAGE_SIZE);
					
					if (pageID <= limit) {
						PageBuffer pageBuffer = new PageBuffer(this.dbName);
						pageBuffer.pageID = pageID;
						pageBuffer.buffer = this.bufMgr.pinBuffer();

						int read = fd.getFileChannel().read(pageBuffer.buffer, pageID * DBPage.PAGE_SIZE);
						if (DBPage.PAGE_SIZE == read) {
							this.put(pageBuffer);
							buffer = pageBuffer;
						}
					}
				}
			} catch (IOException e) {
				System.out.println(e);
				if (null != fd) {
					this.fileMgr.closeFileChannel(fd);
				}
			}
		}

		return buffer;
	}
	
	public void apppend(){

		FileDeco fd = null;
		try {
			fd = this.fileMgr.pinFileChannel(PREFIX_DATA + this.dbName);
	
			if(null != fd){
				long size0 = fd.getFileChannel().size();
				int pos = (int)(fd.getFileChannel().size()/DBPage.PAGE_SIZE) ;	
				
				System.out.println(size0 + ", " + size0/DBPage.PAGE_SIZE + ", " + size0 % DBPage.PAGE_SIZE );
				
//				System.out.println(key + ", append.1, size = " + fd.getFileChannel().size() + ", pos = " + pos);
				ByteBuffer[] array = new ByteBuffer[DBPage.NEW_PAGES_SIZE];
				List<PageBuffer> newBuffList = new ArrayList<PageBuffer>();
				for(int i = 0; i < DBPage.NEW_PAGES_SIZE; ++i){
					PageBuffer pageBuffer = new PageBuffer(this.dbName);
					pageBuffer.pageID = pos + i;
					pageBuffer.buffer = this.bufMgr.pinBuffer(); 
					
					pageBuffer.buffer.rewind();
//					pageBuffer.buffer.putInt(0, pageBuffer.pageID);
//					pageBuffer.buffer.putInt(4, 0);
									
					array[i] = pageBuffer.buffer;
					newBuffList.add(pageBuffer);
				}
				
				fd.getFileChannel().position(fd.getFileChannel().size());
				
				long size = fd.getFileChannel().write(array, 0, DBPage.NEW_PAGES_SIZE);
//				System.out.println(key + ", append.2, size = " + size);
				if(size == DBPage.NEW_PAGES_SIZE * DBPage.PAGE_SIZE){
					
//					System.out.println(key + ", append.3, size = " + this.unusedQueue.size());
					
					for(int i = 0; i < newBuffList.size(); ++i){
						PageBuffer page = newBuffList.get(i);
						this.put(page);
						this.unusedMap.put(page.pageID, page);
						this.unusedQueue.add(page);		
					}
				}
			}				
		} catch (IOException e) {
			System.out.println(e);
			if(null != fd){
				this.fileMgr.closeFileChannel(fd);
			}
		}
	}
	
	public void put(PageBuffer buffer){
		this.pageMap.put(buffer.pageID, buffer);
	}
	
	public PageBuffer pin(int pageID){
		return this.pageMap.get(pageID);
	}
	
	public PageBuffer unpin(int pageID){
		return this.pageMap.remove(pageID);
	}
}

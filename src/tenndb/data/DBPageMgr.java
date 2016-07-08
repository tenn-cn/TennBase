package tenndb.data;

import tenndb.common.FileMgr;

public class DBPageMgr {

	protected PageBufferMgr pageMgr = null;
	
	protected String dbName;
	
	protected FileMgr fileMgr = null;

	protected final Object lock = new Object();
	
	public DBPageMgr(String dbName, FileMgr fileMgr){
		this.dbName  = dbName;
		this.fileMgr = fileMgr;
		
		this.pageMgr  = new PageBufferMgr(this.dbName, fileMgr);
	}
	
	public void load(){
		this.pageMgr.load();
	}

	public void flush(){
		this.pageMgr.flushPage();
		this.pageMgr.flushData();
	}
	
	public PageBuffer getPageBuffer(int pageID){
		PageBuffer buffer = null;
		
		buffer = this.pageMgr.getPageBuffer(pageID);
				
		return buffer;
	}
	
	public synchronized DBBlock getDBBlock(int pageID, int offset){
		DBBlock blk = null;
//		System.out.println("getDBBlock.1");
		PageBuffer buffer = this.getPageBuffer(pageID);
		if(null != buffer){
//			System.out.println("getDBBlock.2");
			blk = buffer.getBlock(offset);
		}else{
//			System.out.println("getDBBlock.3");
		}

		return blk;
	}
	
	public DBBlock nextDBBlock(byte[] buff){

		return this.pageMgr.nextBlock(buff);
	}
}

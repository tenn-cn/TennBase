package tenndb.base;

import java.util.ArrayList;
import java.util.List;

import tenndb.IBase;
import tenndb.bstar.IdxBlock;
import tenndb.common.FileMgr;
import tenndb.data.DBBlock;
import tenndb.data.DBPageMgr;
import tenndb.index.IBTree;
import tenndb.index.IndexMgr;
import tenndb.log.CellLogMgr;
import tenndb.log.LogMgr;
import tenndb.thread.FlushHook;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;


public class Cell implements IBase{

	protected IBTree index = null;
	
	protected final IndexMgr indexMgr;
	protected final DBPageMgr pageMgr;
	
	protected final FileMgr    fileMgr;
	protected final TransMgr   transMgr;
	protected final CellLogMgr logMgr;
	
	protected final String dbName;
	protected final int tabID;
	
	protected FlushHook flushHook = null;
	
	public Cell(String dbName, int tabID, FileMgr fileMgr, TransMgr transMgr, LogMgr logMgr) {
		super();
		this.dbName   = dbName;
		this.tabID    = tabID;
		this.fileMgr  = fileMgr;
		this.transMgr = transMgr;
		this.logMgr   = new CellLogMgr(this.tabID, logMgr);
		this.pageMgr  = new DBPageMgr(this.dbName, fileMgr);
		this.indexMgr = new IndexMgr(this.dbName, fileMgr, transMgr);
	}
	
	public final IBTree getIndex() {
		return index;
	}

	public void init(){
		this.indexMgr.load();
		this.pageMgr.load();
		this.index = this.indexMgr.getBTree();
		
		this.flushHook = new FlushHook(this);
		this.flushHook.start();
	}
	
	public boolean flushNewPages(){
		return this.indexMgr.flushNewPages();	
	}
	
	public void flush(){		
		this.indexMgr.flush();
		this.pageMgr.flush();
	}
	
	public void print(){
		List<String> strList = new ArrayList<String>();
		this.index.print(strList);
		if(null != strList){
			for(String str : strList){
				System.out.println(str);
			}
		}
	}
	
	public void printTreePrior(){
		this.index.printTreePrior();
	}
	
	public void printTreeNext(){
		this.index.printNext();
	}
	
	public boolean insert(int key, String var){
		boolean b = false;
		
		try {
			b = this.insert(key, var, null);
		} catch (AbortTransException e) {
			System.out.println(e);
		}
				
		return b;
	}
	
	public boolean update(int key, String var) {
		boolean b = false;
		
		try {
			b = this.update(key, var, null);
		} catch (AbortTransException e) {
			System.out.println(e);
		}
		
		return b;
	}
	
	public boolean delete(int key){
		boolean b = false;
		try {
			b = this.delete(key, null);
		} catch (AbortTransException e) {
			System.out.println(e);
		}
		return b;
	}
	
	public String search(int key){
		return this.search(key, null);
	}	
	
	public boolean rollback(int key, Trans tid){
		
		return this.index.rollback(key, tid, logMgr);
	}
	
	public boolean commit(int key, Trans tid){
	
		return this.index.commit(key, tid, logMgr);
	}
	
	public boolean recoverDelete(int key){
		boolean b = false;
		IdxBlock blk = this.index.search(key, null);
		if(null != blk){
			blk.setTag(IdxBlock.INVALID);
			b = this.indexMgr.flushBlock(blk); 
		}
		return b;
	}
	
	public boolean recoverInsert(int key, int pageID, int offset){
		boolean b = false;
		IdxBlock blk = this.index.search(key, null);
		if(null != blk){
			blk.setPageID(pageID);
			blk.setOffset(offset);
			b = this.indexMgr.flushBlock(blk); 
		}
		return b;
	}
	
	public boolean recoverUpdate(int key, int pageID, int offset){
		boolean b = false;
		IdxBlock blk = this.index.search(key, null);
		if(null != blk){
			blk.setPageID(pageID);
			blk.setOffset(offset);
			b = this.indexMgr.flushBlock(blk); 
		}
		return b;
	}
	
	public boolean insert(int key, String var, Trans tid) throws AbortTransException{
		boolean b = false;
		if(null != var && var.length() > 0){

			IdxBlock newblk = new IdxBlock(key);
			byte[] buff = var.getBytes();
			DBBlock bblk = this.pageMgr.nextDBBlock(buff);
		//	DBBlock bblk = new DBBlock(null);
			if(null != bblk){
		//		bblk.setVar(buff);
		//		System.out.println(key + ", " + var + ", " + bblk.getPageID() + ", " + bblk.getOffset());
				newblk.setPageID(bblk.getPageID());
				newblk.setOffset(bblk.getOffset());
				newblk.setTag(IdxBlock.VALID);
				
				this.index.insert(key, newblk, tid, this.logMgr);
				
				b = true;
			}	
		}
		return b;
	}
	
	public boolean update(int key, String var, Trans tid) throws AbortTransException{
		boolean b = false;		
		IdxBlock oldblk = null;
		
		if(null != var && var.length() > 0){
			IdxBlock newblk = new IdxBlock(key);
			byte[] buff = var.getBytes();
			DBBlock bblk = this.pageMgr.nextDBBlock(buff);

			if(null != bblk){
				bblk.setVar(var.getBytes());
				
				newblk.setPageID(bblk.getPageID());
				newblk.setOffset(bblk.getOffset());
			
				oldblk = this.index.update(key, newblk, tid, this.logMgr);
				
				if(null != oldblk){
					b = true;
				}
			}
		}

		return b;
	}
	
	public boolean delete(int key, Trans tid) throws AbortTransException{
		boolean b = false;
		IdxBlock oldblk = null;
		
		oldblk = this.index.delete(key, tid, this.logMgr);

		if(null != oldblk){
			b = true;
		}

		return b;
	}
	
	public IdxBlock getIdxBlock(int key){
		return this.index.search(key, null);
	}
	
	public String search(int key, Trans tid){
		String str = null;
		DBBlock bblk = null;
		IdxBlock iblk = this.index.search(key, tid);
		if(null != iblk){
//			System.out.println("search: " + iblk.getTime() + ", " + iblk.getPageID() + ", " + iblk.getOffset() );
			bblk = this.pageMgr.getDBBlock(iblk.getPageID(), iblk.getOffset());
			if(null != bblk){
				str = bblk.getVar();
			}else{
	//			System.out.println("search: " + iblk.getTime() + ", " + iblk.getPageID() + ", " + iblk.getOffset() );
			}
		}
		return str;
	}
	
	public int count(int fromKey, int toKey){
		
		return this.index.count(fromKey, toKey);
	}
	
	public List<String> range(int fromKey, int toKey, boolean isInc){
		
		List<String> bbList = new ArrayList<String>();
		
		List<IdxBlock> idxBList = this.index.range(fromKey, toKey, isInc);
		
		if(null != idxBList && idxBList.size() > 0){
			
			System.out.println("idxBList.size() " + idxBList.size());
			
			DBBlock temp = null;
			for(int i = 0; i < idxBList.size(); ++i){
				IdxBlock blk = idxBList.get(i);
				temp = this.pageMgr.getDBBlock(blk.getPageID(), blk.getOffset());
				if(null != temp){
					bbList.add(temp.getVar());
				}
			}
		}
		
		return bbList;
	}

}

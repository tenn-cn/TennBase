package tenndb.base;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tenndb.common.FileMgr;
import tenndb.data.ByteBufferMgr;
import tenndb.data.DBPage;
import tenndb.log.LogMgr;
import tenndb.tx.BlkID;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;

public class Catalog {
    
	protected final String cataName;	
    protected final String root;
    
    protected final FileMgr       fileMgr;
    protected final LogMgr        logMgr;
	protected final TransMgr      transMgr;

    protected final ByteBufferMgr bufMgr;
    protected Map<String, Cell>   tabNameToCells = null;
    protected Map<Integer, Cell>  tabIdToCells = null;
   
    
    
	protected final ReadWriteLock lock = new ReentrantReadWriteLock(false);

	public Catalog(){
		this.cataName = "tennbase";
		this.root     = "J:\\tennbase";
		this.fileMgr  = new FileMgr(this.root);
		
		this.logMgr   		= new LogMgr(this.cataName, this.root);
		this.bufMgr   		= new ByteBufferMgr(DBPage.PAGE_SIZE);
		this.tabNameToCells = new Hashtable<String,  Cell>();
		this.tabIdToCells   = new Hashtable<Integer, Cell>();
		this.transMgr 		= new TransMgr();
	}
	
	public Trans beginTrans(){
		Trans trans = new Trans();
		
		this.logMgr.logBegin(trans);
		this.transMgr.setTransState(trans, Trans.ACTIVE);
		
		return trans;
	}
		
	public void recover(){
		this.logMgr.recover(this);
		
	}
	public void flush(){
		
		try{
			this.lockRead();
			Set<String> set = this.tabNameToCells.keySet();
			for(String dbName : set){
				Cell cell = this.tabNameToCells.get(dbName);
				cell.flush();
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
	}
	
	public boolean rollback(Trans tid){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0 && null != this.transMgr){
			try{
				this.logMgr.logRollback(tid);
				this.lockRead();
				ArrayList<BlkID> blks = this.transMgr.getTransBlks(tid);
				if(null != blks && blks.size() > 0){
					
					for(int i = blks.size() - 1; i >= 0; --i){
						BlkID blk = blks.get(i);
						Cell cell = this.tabNameToCells.get(blk.getDbName());
						if(null != cell){
							cell.rollback(blk.getKey(), tid);
						}
					}				
				}
				this.transMgr.delTransBlks(tid);
				b = true;
			}catch(Exception e){}
			finally{
				this.unLockRead();
			}
			this.logMgr.logTerminated(tid);
		}
		
		return b;
	}
	
	public byte state(Trans tid){
		return this.transMgr.getTransState(tid);
	}
	
	public boolean commit(Trans tid){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0 && null != this.transMgr){
			
			if(tid.getState() == Trans.ACTIVE){
				try{					
					this.logMgr.logCommit(tid);
					
					this.lockRead();
					ArrayList<BlkID> blks = this.transMgr.getTransBlks(tid);
					if(null != blks && blks.size() > 0){
						
						for(int i = blks.size() - 1; i >= 0; --i){
							BlkID blk = blks.get(i);
							Cell cell = this.tabNameToCells.get(blk.getDbName());
							if(null != cell){
								cell.commit(blk.getKey(), tid);
							}
						}						
					}
					this.transMgr.delTransBlks(tid);
					b = true;
				}catch(Exception e){}
				finally{
					this.unLockRead();
				}
				this.logMgr.logTerminated(tid);
			}
		}
		
		return b;
	}
	
	public boolean addCell(String dbName, int dbID){
		boolean b = false;
		if(!tabNameToCells.containsKey(dbName)){
			Cell cell = new Cell(dbName, dbID, this.fileMgr, this.transMgr, this.logMgr);
			cell.init();
			System.out.println("addCell " + dbName);
//			cell.print();
			
			try{
				this.lockWrite();
				this.tabNameToCells.put(dbName, cell);
				this.tabIdToCells.put(dbID, cell);
				b = true;
			}catch(Exception e){}
			finally{
				this.unLockWrite();
			}
		}
		return b;
	}
	
	public final Cell getCell(int dbID){
		return this.tabIdToCells.get(dbID);
	}
	
	public final Cell getCell(String dbName){
		return this.tabNameToCells.get(dbName);
	}
	
	protected void lockRead()    { this.lock.readLock().lock(); }
	
	protected void unLockRead()  { this.lock.readLock().unlock(); }

	protected void lockWrite()   { this.lock.writeLock().lock(); }
	
	protected void unLockWrite() { this.lock.writeLock().unlock(); }
	
	
}

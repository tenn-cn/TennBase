package tenndb.log;

import tenndb.bstar.IdxBlock;
import tenndb.tx.Trans;

public class CellLogMgr {

	protected final int tabID;
	
	protected final LogMgr logMgr;

	public CellLogMgr(int tabID, LogMgr logMgr) {
		super();
		this.tabID  = tabID;
		this.logMgr = logMgr;
	}

	public boolean logBegin(Trans tid){
		return this.logMgr.logBegin(tid);
	}
	
	public boolean logRollback(Trans tid){
		return this.logMgr.logRollback(tid);
	}
	
	public boolean logCommit(Trans tid){
		return this.logMgr.logCommit(tid);
	}
	
	public boolean logTerminated(Trans tid){
		return this.logMgr.logTerminated(tid);
	}
	
	public boolean logInsert(Trans tid, int key, IdxBlock newblk){
		return this.logMgr.logInsert(tid, key, this.tabID, newblk);
	}
	
	public boolean logDelete(Trans tid, int key){
		return this.logMgr.logDelete(tid, key, this.tabID);
	}
	
	public boolean logUpdate(Trans tid, int key, IdxBlock newblk, IdxBlock oldblk){
		return this.logMgr.logUpdate(tid, key, this.tabID, newblk, oldblk);
	}
}

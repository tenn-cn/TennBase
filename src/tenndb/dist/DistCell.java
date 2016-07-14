package tenndb.dist;

import java.util.List;

import tenndb.IBase;
import tenndb.common.FileMgr;
import tenndb.data.Colunm;
import tenndb.data.DBPageMgr;
import tenndb.index.IBTree;
import tenndb.index.IndexMgr;
import tenndb.log.CellLogMgr;
import tenndb.log.LogMgr;
import tenndb.thread.FlushHook;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;

public class DistCell {

	protected IBTree index = null;
	
	protected final IndexMgr indexMgr;
	protected final DBPageMgr pageMgr;
	
	protected final FileMgr    fileMgr;
	protected final TransMgr   transMgr;
	protected final CellLogMgr logMgr;
	
	protected final String dbName;
	protected final int tabID;
	
	protected FlushHook flushHook = null;
	
	public DistCell(String dbName, int tabID, FileMgr fileMgr, TransMgr transMgr, LogMgr logMgr) {
		super();
		this.dbName   = dbName;
		this.tabID    = tabID;
		this.fileMgr  = fileMgr;
		this.transMgr = transMgr;
		this.logMgr   = new CellLogMgr(this.tabID, logMgr);
		this.pageMgr  = new DBPageMgr(this.dbName, fileMgr);
		this.indexMgr = new IndexMgr(this.dbName, fileMgr, transMgr);
	}
	


}

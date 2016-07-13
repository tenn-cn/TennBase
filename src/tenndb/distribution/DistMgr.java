package tenndb.distribution;

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tenndb.base.Cell;
import tenndb.common.FileMgr;
import tenndb.data.ByteBufferMgr;
import tenndb.data.DBPage;
import tenndb.log.LogMgr;
import tenndb.tx.TransMgr;

public class DistMgr {

	protected Cell devs = null;

    protected final String        root;
	protected final String        cataName;	
    protected final FileMgr       rootMgr;
    protected final LogMgr        logMgr;
	protected final TransMgr      transMgr;
    protected final ByteBufferMgr bufMgr;
	
    protected volatile boolean initialized = false;
	protected final ReadWriteLock lock = new ReentrantReadWriteLock(false);
	
	public static final String DIST_PATH = System.getProperty("file.separator") + "dist_data";
	
	public DistMgr(String root){
		this.cataName       = "distribution";
		this.root           = root;
		this.rootMgr        = new FileMgr(this.root);
		
		this.logMgr   		= new LogMgr(this.cataName, this.root);
		this.bufMgr   		= new ByteBufferMgr(DBPage.PAGE_SIZE);
		this.transMgr 		= new TransMgr();
		
		this.devs           = new Cell(this.cataName, 0, this.rootMgr, this.transMgr, this.logMgr);
	}
	
	public void init(){
		if(false == this.initialized){
			try{
				this.lock.writeLock().lock();
				if(false == this.initialized){
					this.devs.init();
					this.initialized = true;
				}
			}catch(Exception e){
				System.out.println(e);
			}finally{
				this.lock.writeLock().unlock();
			}
		}
	}
	
	
}

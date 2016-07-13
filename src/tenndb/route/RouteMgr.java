package tenndb.route;

import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tenndb.base.Cell;
import tenndb.common.DateFormatUtil;
import tenndb.common.FileMgr;
import tenndb.data.ByteBufferMgr;
import tenndb.data.Colunm;
import tenndb.data.DBPage;
import tenndb.log.LogMgr;
import tenndb.tx.TransMgr;



public class RouteMgr {

	protected Cell level0 = null;
	
    protected Map<String,  Cell>  level1  = null;
    protected Map<String,  Cell>  level2  = null;
    
    protected final String        root;
	protected final String        cataName;	
    protected final FileMgr       rootMgr;
    protected final LogMgr        logMgr;
	protected final TransMgr      transMgr;
    protected final ByteBufferMgr bufMgr;
    
    protected volatile boolean initialized = false;
	protected final ReadWriteLock lock = new ReentrantReadWriteLock(false);
	
	private static final ThreadLocal<SimpleDateFormat> DATA_PATH_FORMAT =
		DateFormatUtil.threadLocalDateFormat("yyyy" + System.getProperty("file.separator") + "MM"+ System.getProperty("file.separator") + "dd");
	
	public RouteMgr(String root){
		this.cataName       = "date_route";
		this.root           = root;
		this.rootMgr        = new FileMgr(this.root);
		
		this.logMgr   		= new LogMgr(this.cataName, this.root);
		this.bufMgr   		= new ByteBufferMgr(DBPage.PAGE_SIZE);
		this.transMgr 		= new TransMgr();
		
		this.level0         = new Cell(this.cataName, 0, this.rootMgr, this.transMgr, this.logMgr);
		this.level1         = new Hashtable<String, Cell>();
		this.level2         = new Hashtable<String, Cell>();
	}
	
	public void init(){
		if(false == this.initialized){
			try{
				this.lock.writeLock().lock();
				if(false == this.initialized){
					this.level0.init();
				}
			}catch(Exception e){
				System.out.println(e);
			}finally{
				this.lock.writeLock().unlock();
			}
		}
	}
	public final Cell getLevel0(){
		return this.level0;
	}
	
	public static final String data2path(String date){
		String path = null;
		
		if(null != date && date.length() == 6){
			path = System.getProperty("file.separator") + date.substring(0, 2) 
			     + System.getProperty("file.separator") + date.substring(2, 4) 
			     + System.getProperty("file.separator") + date.substring(4, 6) ;
		}
		
		return path;
	}
	
	public static final String dev2path(String dev){
		String path = null;
		
		if(null != dev && dev.length() == 10){
			path = System.getProperty("file.separator") + dev.substring(8, 10) ;
		}
		
		return path;
	}
	
	public final Cell pinLevel2(String level1, String level2){
		Cell cell2 = null;
		String key = level1 + "_" + level2;
		cell2 = this.level2.get(key);
		
		if(null == cell2){
			Cell cell1 = this.pinLevel1(level1);
			if(null != cell1){
				Colunm colunm = cell1.search(Colunm.hashCode(level2));
				if(null == colunm){
					colunm = new Colunm(key, 1);						
					cell1.insert(colunm.getHashCode(), colunm);
				}
				
				if(null != colunm){
					String key2 = colunm.getKey();
					if(null != key2){
						String datepath = data2path(level1);
						String devpath  = dev2path (level2);
						//dev2path
						cell2 = new Cell(key2, 0, new FileMgr(this.root + datepath + devpath), this.transMgr, this.logMgr);	
						cell2.init();
						this.level2.put(key2, cell2);
					}
				}
			}
		}

		return cell2;
	}
	
	//160101
	public final Cell pinLevel1(String level1){
		
		Cell cell = null;
				
		if(null != level1 && level1.length() > 0){
			cell = this.level1.get(level1);
			if(null == cell){
				Colunm colunm = this.level0.search(Colunm.hashCode(level1));
				if(null == colunm){
					colunm = new Colunm(level1, 1);						
					this.level0.insert(colunm.getHashCode(), colunm);
				}
				
				if(null != colunm){
					String key = colunm.getKey();
					if(null != key){
						String path = data2path(key);
						cell = new Cell(key, 0, new FileMgr(this.root + path), this.transMgr, this.logMgr);	
						cell.init();
						this.level1.put(key, cell);
					}			
				}
			}
		}
		
		return cell;
	}
	
	protected void lockRead()    { this.lock.readLock().lock();    }
	
	protected void unLockRead()  { this.lock.readLock().unlock();  }

	protected void lockWrite()   { this.lock.writeLock().lock();   }
	
	protected void unLockWrite() { this.lock.writeLock().unlock(); }
	
}

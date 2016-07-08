package tenndb.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tenndb.base.Catalog;
import tenndb.base.Cell;
import tenndb.bstar.IdxBlock;
import tenndb.tx.Trans;

public class LogMgr {

    static final byte  START_RECORD      =  1;

    static final byte  INSERT_RECORD     =  2;
    static final byte  UPDATE_RECORD     =  3;
    static final byte  DELETE_RECORD     =  4;
    
    static final byte  ROLLBACK_RECORD   =  5;
    static final byte  COMMIT_RECORD     =  6;
    static final byte  TERMINATED_RECORD =  7;
    
    static final byte  CHECKPOINT_RECORD =  8;
    static final byte  NO_CHECKPOINT_ID  =  0;
    
	
	public static final int INT_SIZE    = Integer.SIZE / Byte.SIZE;
	public static final int SHORT_SIZE  = Short.SIZE / Byte.SIZE;
	public static final int BYTE_SIZE   = Byte.SIZE / Byte.SIZE;
	
	//tid        4
	//type       1
	//state      1
	//new.key    4
	//tabId      4
	//new.pageid 4
	//new.offset 2
	//new.tag    1
	//old.pageid 4 
	//old.offset 2
	//old.tag    1
	//prelog.pos 4	
	
	public static final int LOG_BLOCK_SIZE = INT_SIZE + BYTE_SIZE  + BYTE_SIZE + INT_SIZE + INT_SIZE 
										   + INT_SIZE + SHORT_SIZE + BYTE_SIZE  
										   + INT_SIZE + SHORT_SIZE + BYTE_SIZE 
										   + INT_SIZE;
	
	protected boolean logging = true;
	protected final File logFile;
	protected RandomAccessFile raf = null;
	private Map<Integer, Integer> tidToTailLogRecord = null;
	
	protected final ReadWriteLock lock = new ReentrantReadWriteLock(false);

	protected static final String PREFIX_LOG = "log_";
	
	
	public void recover(Catalog catalog){
		List<LogRecord> logRecordList = this.loadLogRecord();
		if(null != logRecordList){
			Map<Integer, List<LogRecord>> undoSetMap = new HashMap<Integer, List<LogRecord>>();
			Map<Integer, List<LogRecord>> redoSetMap = new HashMap<Integer, List<LogRecord>>();
			
			this.recoverLogRecord(logRecordList, undoSetMap, redoSetMap);
			
			System.out.println("undo.size = " + undoSetMap.size());
			System.out.println("redo.size = " + redoSetMap.size());
			
			this.redo(catalog, redoSetMap);
			this.undo(catalog, undoSetMap);			
		}
		
		this.truncate();
	}
	
	public void undo(Catalog catalog, Map<Integer, List<LogRecord>> undoSetMap){
		
	}
	
	public void redo(Catalog catalog, Map<Integer, List<LogRecord>> redoSetMap){
		Set<Integer> tidSet = redoSetMap.keySet();
		for(Integer tid : tidSet){
			List<LogRecord> list = redoSetMap.get(tid);
			if(null != list && list.size() > 0){
				for(int i = list.size() - 1; i >= 0; --i){
					LogRecord record = list.get(i);
					
					int tabID = record.tabId;
					Cell cell = catalog.getCell(tabID);
					if(null != cell){
						if(INSERT_RECORD == record.type){
							cell.recoverInsert(record.key, record.getNewPid(), record.getNewOffset());
						}else if(UPDATE_RECORD == record.type){
							cell.recoverUpdate(record.key, record.getNewPid(), record.getNewOffset());
						}else if(DELETE_RECORD == record.type){
							cell.recoverDelete(record.key);
						}
					}
				}
			}
		}
	}
	
	public void truncate(){
		System.out.println("truncate.1");
		if(this.logFile.exists()){
			try {
				this.raf.close();
				System.out.println("truncate.2");
				this.logFile.delete();
				this.raf = new RandomAccessFile(this.logFile, "rw");			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("truncate.3");
	}
	
	public void recoverLogRecord(List<LogRecord> logRecordList, Map<Integer, List<LogRecord>> undoSetMap, Map<Integer, List<LogRecord>> redoSetMap){
		
		Map<Integer, Byte> recordMap = new HashMap<Integer, Byte>();
		Set<Integer> undoSet = new HashSet<Integer>();
		Set<Integer> redoSet = new HashSet<Integer>();
		
		for(int i = logRecordList.size() - 1; i >= 0 ; --i){
			LogRecord record = logRecordList.get(i);
			if(!recordMap.containsKey(record.tid)){
				if(Trans.COMMIT == record.type){
					redoSet.add(record.tid);
				}else if(Trans.ROLLBACK == record.type){
					undoSet.add(record.tid);
				}else if(Trans.ACTIVE == record.type){
					undoSet.add(record.tid);
				}
				
				recordMap.put(record.tid, record.type);
			}else{
				if(Trans.START == record.type){
					undoSet.remove(record.tid);
					redoSet.remove(record.tid);
				}
			}
			
			if(redoSet.contains(record.tid)){
				List<LogRecord> list = redoSetMap.get(record.tid);
				if(null == list){
					list = new ArrayList<LogRecord>();
					redoSetMap.put(record.tid, list);
				}
				list.add(record);
			}
			
			if(undoSet.contains(record.tid)){
				List<LogRecord> list = undoSetMap.get(record.tid);
				if(null == list){
					list = new ArrayList<LogRecord>();
					undoSetMap.put(record.tid, list);
				}
				list.add(record);
			}
		}		
	}
	
	public LogMgr(String cataName, String root){
		File file    = new File(root, PREFIX_LOG + cataName);
		this.logFile = file;
		this.logging = true;
		this.tidToTailLogRecord = new Hashtable<Integer, Integer>();
		try {
			this.raf  = new RandomAccessFile(this.logFile, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public List<LogRecord> loadLogRecord(){
		List<LogRecord> list = new ArrayList<LogRecord>();
		
		try{
			byte[] array = new byte[LOG_BLOCK_SIZE];
			ByteBuffer buff = ByteBuffer.allocate(LOG_BLOCK_SIZE);
			this.lockWrite();
			
			this.raf.seek(0);
			
			while(this.raf.getFilePointer() < this.raf.length()){
				this.raf.read(array);	
				buff.flip();
				buff.clear();
				buff.put(array);
				//tid        4
				//type       1
				//state      1
				//new.key    4
				//tabId      4
				//new.pageid 4
				//new.offset 2
				//new.tag    1
				//old.pageid 4 
				//old.offset 2
				//old.tag    1
				//prelog.pos 4				
				buff.flip();
				
				int tid 		= buff.getInt(); 
				byte type 		= buff.get();
				byte state      = buff.get();
				int key 		= buff.getInt(); 
				int tabId 		= buff.getInt();
				int newPid 		= buff.getInt(); 
				short newOffset = buff.getShort(); 
				byte newTag 	= buff.get();
				int oldPid 		= buff.getInt(); 
				short oldOffset = buff.getShort(); 
				byte oldTag 	= buff.get();
				int pos         = buff.getInt();
				
				LogRecord record = new LogRecord(tid, state, type, key, tabId, 
						  						 newPid, newOffset, newTag, 
						  						 oldPid, oldOffset, oldTag);
				record.prePos = pos;
				record.state  = state;
				list.add(record);
			}						
		}catch(Exception e){}
		finally{
			this.unLockWrite();
		}
		
		return list;
	}

	public boolean writeLog(LogRecord record){
		boolean b = false;
		
//		this.logCache.push(record);
		
		if(null != record && record.tid > 0 && true == this.logging){
			try{
				this.lockWrite();
				int start = (int) this.raf.length();
				
				this.raf.seek(start);
				this.raf.writeInt  (record.tid);
				this.raf.writeByte (record.type);
				this.raf.writeByte (record.state);
				this.raf.writeInt  (record.key);
				this.raf.writeInt  (record.tabId);
				
				this.raf.writeInt  (record.newPid);
				this.raf.writeShort(record.newOffset);
				this.raf.writeByte (record.newTag);
				
				this.raf.writeInt  (record.oldPid);
				this.raf.writeShort(record.oldOffset);
				this.raf.writeByte (record.oldTag);
				
				if(START_RECORD != record.type){
					Integer pos = this.tidToTailLogRecord.get(record.tid);

					this.raf.writeInt(pos);

				}else{
					this.raf.writeInt(0);
				}
				
				this.tidToTailLogRecord.put(record.tid, start);
				
				b = true;
			}catch(Exception e){}
			finally{
				this.unLockWrite();
			}
		}
		
		return b;
	}
	
	public boolean logBegin(Trans tid){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0){
			LogRecord record = new LogRecord(tid.getTransID(), Trans.START, START_RECORD, 0, 0, 
											 0, (short)0, (byte)0, 0, (short)0, (byte)0);		
			b = this.writeLog(record);
		}
		return b;
	}
	
	public boolean logTerminated(Trans tid){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0){
			LogRecord record = new LogRecord(tid.getTransID(), Trans.TERMINATED, TERMINATED_RECORD, 0, 0, 
											 0, (short)0, (byte)0, 0, (short)0, (byte)0);			
			b = this.writeLog(record);
		}
		return b;
	}
	
	public boolean logRollback(Trans tid){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0){
			LogRecord record = new LogRecord(tid.getTransID(), Trans.ROLLBACK, ROLLBACK_RECORD, 0, 0, 
											 0, (short)0, (byte)0, 0, (short)0, (byte)0);			
			b = this.writeLog(record);
		}
		return b;
	}
	
	public boolean logCommit(Trans tid){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0){		
			LogRecord record = new LogRecord(tid.getTransID(), Trans.COMMIT, COMMIT_RECORD, 0, 0, 
											 0, (short)0, (byte)0, 0, (short)0, (byte)0);			
			b = this.writeLog(record);
		}
		return b;
	}
	
	public boolean logInsert(Trans tid, int key, int tabId, IdxBlock newblk){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0 && null != newblk){
			LogRecord record = new LogRecord(tid.getTransID(), Trans.ACTIVE, INSERT_RECORD, key, tabId, 
											 newblk.getPageID(), (short)newblk.getOffset(), (byte)newblk.getTag(), 
											 (int)0, (short)0, (byte)0);
			b = this.writeLog(record);
		}
		return b;
	}
	
	public boolean logDelete(Trans tid, int key, int tabId){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0){
			LogRecord record = new LogRecord(tid.getTransID(), Trans.ACTIVE, DELETE_RECORD, key, tabId, 
											 0, (short)0, (byte)IdxBlock.INVALID, 
											 0, (short)0, (byte)IdxBlock.VALID);
			b = this.writeLog(record);
		}
		
		return b;
	}
	
	public boolean logUpdate(Trans tid, int key, int tabId, IdxBlock newblk, IdxBlock oldblk){
		boolean b = false;
		
		if(null != tid && tid.getTransID() > 0 && null != newblk && null != oldblk){
			LogRecord record = new LogRecord(tid.getTransID(), Trans.ACTIVE, UPDATE_RECORD, key, tabId, 
											 newblk.getPageID(), (short)newblk.getOffset(), (byte)newblk.getTag(), 
											 oldblk.getPageID(), (short)oldblk.getOffset(), (byte)oldblk.getTag());
			b = this.writeLog(record);
		}
		
		return b;
	}
	
	
	protected void lockRead()    { this.lock.readLock().lock(); }
	
	protected void unLockRead()  { this.lock.readLock().unlock(); }

	protected void lockWrite()   { this.lock.writeLock().lock(); }
	
	protected void unLockWrite() { this.lock.writeLock().unlock(); }
}

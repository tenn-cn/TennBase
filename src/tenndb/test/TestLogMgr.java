package tenndb.test;

import java.util.List;

import tenndb.bstar.IdxBlock;
import tenndb.log.LogMgr;
import tenndb.log.LogRecord;
import tenndb.tx.Trans;

public class TestLogMgr {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		LogMgr logMgr = new LogMgr("tenn", "J:\\tennbase");

		Trans tid = new Trans();
		
		logMgr.logBegin(tid);
		
		for(int i = 1; i <= 10; ++i){
			IdxBlock newblk = new IdxBlock(i);
			newblk.setPageID(3);
			newblk.setOffset(i);
			newblk.setTag(IdxBlock.VALID);
			logMgr.logInsert(tid, i, 1, newblk);
		}
		
		for(int i = 1; i <= 10; ++i){
			IdxBlock newblk = new IdxBlock(i);
			newblk.setPageID(3);
			newblk.setOffset(i);
			newblk.setTag(IdxBlock.VALID);
			
			IdxBlock oldblk = new IdxBlock(i);
			oldblk.setPageID(4);
			oldblk.setOffset(i);
			oldblk.setTag(IdxBlock.VALID);
			
			logMgr.logUpdate(tid, i, 1, newblk, oldblk);
		}
		
		logMgr.logCommit(tid);
		
		List<LogRecord> list = logMgr.loadLogRecord();
		
		if(null != list){
			for(int i = 0; i < list.size(); ++i){
				LogRecord record = list.get(i);
				
				System.out.println("tid = " + record.getTid() + ", type = " + record.getType() + ", key = " + record.getKey() 
						+ ", state = " + record.getState() + ", pos = " + record.getPrePos() 
						+ ", nPid = " + record.getNewPid() + ", nOffSet = " + record.getNewOffset() + ", nTag = " + record.getNewTag() 
						+ ", oPid = " + record.getOldPid() + ", oOffSet = " + record.getOldOffset() + ", oTag = " + record.getOldTag());
			}
		}
	}

}

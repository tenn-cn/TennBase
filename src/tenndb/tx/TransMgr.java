package tenndb.tx;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class TransMgr {
	
	protected ConcurrentHashMap<String, Trans> blkTrans;
	
	protected ConcurrentHashMap<Trans, ArrayList<BlkID>>  transBlks;
	
	protected ConcurrentHashMap<Trans, Byte> transState;
	
	public TransMgr(){

		this.blkTrans   = new ConcurrentHashMap<String, Trans>();
		this.transBlks  = new ConcurrentHashMap<Trans, ArrayList<BlkID>>();	
		this.transState = new ConcurrentHashMap<Trans, Byte>();
	}
	
	protected ReadWriteLock lock = new ReentrantReadWriteLock(false);
	
	protected void lockRead()    { this.lock.readLock().lock(); }
	
	protected void unLockRead()  { this.lock.readLock().unlock(); }

	protected void lockWrite()   { this.lock.writeLock().lock(); }
	
	protected void unLockWrite() { this.lock.writeLock().unlock(); }
	
	
	public synchronized void delTransBlks(Trans tid){
		if(null != tid){			
			ArrayList<BlkID> blkList = this.transBlks.get(tid);
			
			if(null != blkList){
				for(BlkID blk : blkList){
					this.blkTrans.remove(blk.getSeq());
				}
			}
			
			this.transBlks.remove(tid);
			this.transState.remove(tid);
		}
	}
	
	public synchronized void setTransState(Trans tid, byte state){
		if(null != tid){
			this.transState.put(tid, state);
		}
	}
	
	public synchronized byte getTransState(Trans tid){
		byte state = Trans.INVALID;
		
		if(null != tid){
			Byte var = this.transState.get(tid);
			if(null != var){
				state = var.byteValue();
			}
		}
		return state;
	}
	
	public synchronized ArrayList<BlkID> getTransBlks(Trans tid){
		ArrayList<BlkID> blkList = null;
		
		if(null != tid){
			blkList = this.transBlks.get(tid);
		}
		return blkList;
	}
	
	public synchronized boolean unlockBlk(Trans tid){
		boolean b = false;
		
		if(null != tid){
			this.transBlks.remove(tid);
			ArrayList<BlkID> blkList = this.transBlks.get(tid);
			if(null != blkList && blkList.size() > 0){
				for(BlkID blk : blkList){
					this.blkTrans.remove(blk.getSeq());	
				}				
			}
			b = true;
		}
		
		return b;
	}
	
/*	public synchronized boolean lockBlk(BlkID bid, Trans tid){
		boolean b = false;
		
		if(null != bid && null != tid){
			Trans trans = this.blkTrans.get(bid.getSeq());
			if(null == trans || trans.equals(tid)){
			
				if(null == trans){
					this.blkTrans.put(bid.getSeq(), tid);
				}
				
				ArrayList<BlkID> blkList = this.transBlks.get(tid);
				
				if(null == blkList){
					blkList = new ArrayList<BlkID>();
					this.transBlks.put(tid, blkList);
				}
				boolean had = false;
				for(BlkID tmp : blkList){
					if(tmp.getSeq().equals(bid.getSeq())){
						had = true;
						break;
					}
				}
				
				if(false == had){
//					System.out.println("lockBlk " + bid.getSeq());
					blkList.add(bid);
				}
				
				b = true;
			}
		}
				
		return b;
	}*/
	
	public synchronized boolean lockBlk(BlkID bid, Trans tid){
		boolean b = false;
		
		if(null != bid && null != tid){
			Trans trans = this.blkTrans.get(bid.getSeq());
			if(null == trans || trans.equals(tid)){
			
				if(null == trans){
					this.blkTrans.put(bid.getSeq(), tid);
				}
				
				ArrayList<BlkID> blkList = this.transBlks.get(tid);
				
				if(null == blkList){
					blkList = new ArrayList<BlkID>();
					this.transBlks.put(tid, blkList);
				}
				blkList.add(bid);
				
				b = true;
			}
		}
				
		return b;
	}	
}

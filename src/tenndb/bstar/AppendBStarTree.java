package tenndb.bstar;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tenndb.index.IBTree;
import tenndb.index.IndexMgr;
import tenndb.log.CellLogMgr;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;

public class AppendBStarTree implements IBTree {

	public final static int BALANCE_SIZE = 256;
	
	protected String    dbName;
	protected final int size;
	protected BTreeNode root;
	protected IndexMgr  indexMgr;
	protected TransMgr  transMgr;
	
	protected ReadWriteLock lock = new ReentrantReadWriteLock(false);
	
	public void lockRead()    { this.lock.readLock().lock(); }
	
	public void unLockRead()  { this.lock.readLock().unlock(); }

	public void lockWrite()   { this.lock.writeLock().lock(); }
	
	public void unLockWrite() { this.lock.writeLock().unlock(); }
	
	
	public AppendBStarTree(String dbName, IndexMgr indexMgr, TransMgr transMgr) {
		super();
		this.dbName   = dbName;
		this.size     = BALANCE_SIZE;
		this.indexMgr = indexMgr;
		this.transMgr = transMgr;
	}
	
	public AppendBStarTree(String dbName, IndexMgr indexMgr, TransMgr transMgr, BTreeNode root) {
		super();
		this.dbName   = dbName;
		this.size     = BALANCE_SIZE;
		this.indexMgr = indexMgr;
		this.transMgr = transMgr;		
		this.root     = root;
	}
	
	
	@Override
	public BTreeNode getRoot() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void print(List<String> strList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void printNext() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void printTreePrior() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IdxBlock insert(int key, IdxBlock var, Trans tid, CellLogMgr logMgr)
			throws AbortTransException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IdxBlock update(int key, IdxBlock var, Trans tid, CellLogMgr logMgr)
			throws AbortTransException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IdxBlock delete(int key, Trans tid, CellLogMgr logMgr)
			throws AbortTransException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IdxBlock search(int key, Trans tid) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean rollback(int key, Trans tid, CellLogMgr logMgr) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean commit(int key, Trans tid, CellLogMgr logMgr) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public BTreeNode seekNode(int key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int count(int fromKey, int toKey) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<IdxBlock> range(int fromKey, int toKey, boolean isInc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void toByteBuffer(List<ByteBuffer> list) {
		// TODO Auto-generated method stub
		
	}

}

package tenndb;

import java.util.List;

import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;

public interface IBase {
	
	
	public boolean insert(int key, String var, RefVar t);
	
	public boolean update(int key, String var);
	
	public boolean delete(int key);
	
	public String  search(int key);
	
	
	public boolean insert(int key, String var, Trans tid, RefVar t) throws AbortTransException;
	
	public boolean update(int key, String var, Trans tid) throws AbortTransException;
	
	public boolean delete(int key, Trans tid) throws AbortTransException;
	
	public boolean rollback(int key, Trans tid);
	
	public boolean commit(int key, Trans tid);

	public String  search(int key, Trans tid);
	
	public int count(int fromKey, int toKey);
	
	public List<String> range(int fromKey, int toKey, boolean isInc);
	
	public void printTreeNext();
	
	public void printTreePrior();
	
}

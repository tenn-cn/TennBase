package tenndb;

import java.util.List;

import tenndb.data.Colunm;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;

public interface IBase {
	
	public boolean insert(int key, byte[] buff, int offset, int len);
	
	public boolean update(int key, byte[] buff, int offset, int len);

	
	public boolean insert(int key, Colunm colunm);
	
	public boolean update(int key, Colunm colunm);
	
	public boolean delete(int key);
	
	public Colunm  search(int key);
	
	
	public boolean insert(int key, Colunm colunm, Trans tid) throws AbortTransException;
	
	public boolean update(int key, Colunm colunm, Trans tid) throws AbortTransException;
	
	public boolean delete(int key, Trans tid) throws AbortTransException;
	
	public boolean rollback(int key, Trans tid);
	
	public boolean commit(int key, Trans tid);

	public Colunm  search(int key, Trans tid);
	
	public int count(int fromKey, int toKey);
	
	public List<Colunm> range(int fromKey, int toKey, boolean isInc);
	
	public void printTreeNext();
	
	public void printTreePrior();
	
}

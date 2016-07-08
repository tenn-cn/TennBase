package tenndb.tx;

import java.io.Serializable;

import tenndb.index.IndexPage;

public class BlkID implements Serializable {

	private static final long serialVersionUID = -2731980102685155241L;
	
	protected String dbName;
	protected int key;
	
	protected IndexPage page;
	protected Trans transactionID;
	
	public BlkID(String dbName, int key) {
		super();
		this.dbName = dbName;
		this.key    = key;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public IndexPage getPage() {
		return page;
	}

	public void setPage(IndexPage page) {
		this.page = page;
	}

	public Trans getTransactionID() {
		return transactionID;
	}

	public void setTransactionID(Trans transactionID) {
		this.transactionID = transactionID;
	}

	public int getKey(){
		return this.key;
	}
	
	public String getSeq(){
		return this.dbName + "_" + this.key;
	}
}

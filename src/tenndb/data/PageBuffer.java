package tenndb.data;

import java.nio.ByteBuffer;


public class PageBuffer {

	protected int pageID;
	
	protected int offset;
	
	protected int size;
	
	protected ByteBuffer buffer = null;
	
	protected int tableID;
	
	protected String tableName;
	
	public PageBuffer(String tableName){
		this.pageID    = 0;
		this.offset    = 0;
		this.tableName = tableName;
		this.size      = DBPage.PAGE_SIZE;
		this.tableID   = 0;
	}
	
	public String getTableName(){
		return this.tableName;
	}
	
	public int getTableID(){
		return this.tableID;
	}
	
	public int getPageID() {
		return this.pageID;
	}

	public void setPageID(int pageID) {
		this.pageID = pageID;
	}

	public int getOffset() {
		return this.offset;
	}

	public int getSize() {
		return size;
	}
	
	public  boolean isfull(byte[] buff){
		return (this.offset + DBPage.BLOCK_SIZE + buff.length) > this.size;
	}
	
	public  DBBlock nextBlock(byte[] buff){
		DBBlock blk = null;
		if((this.offset + DBPage.BLOCK_SIZE + buff.length) <= this.size){
			blk = this.getBlock(this.offset);
			this.offset += DBPage.BLOCK_SIZE + buff.length;
		}
		return blk;
	}
	
	public void setString(int offset, String var){
		DBBlock block = this.getBlock(offset);
		
		if(null != block){
			block.setVar(var);
		}
	}
	
	public void setBlock(byte[] var, int offset){
		DBBlock block = this.getBlock(offset);
		
		if(null != block){
			block.setVar(var);
		}
	}
	
	public String getString(int offset){
		String str = null;
		DBBlock blk = this.getBlock(offset);
		if(null != blk){
			str = blk.getVar();
		}
		return str;
	}
	
	public DBBlock getBlock(int offset){
		DBBlock blk = null;
		
		if(offset < this.size){			
			blk = new DBBlock(this);
			blk.setOffset(offset);
			blk.setPageID(this.pageID);

		}

		return blk;
	}
}

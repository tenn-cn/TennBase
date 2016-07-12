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
	
	public  boolean isfull(Colunm colunm){
		return (this.offset + DBBlock.HEAD_SIZE + colunm.len) > this.size;
	}
	
	public  DBBlock nextBlock(Colunm colunm){
		DBBlock blk = null;
		if((this.offset + DBBlock.HEAD_SIZE + colunm.len) <= this.size){
			blk = this.getBlock(this.offset);
			blk.setColunm(colunm);
			this.offset += DBBlock.HEAD_SIZE + colunm.len;
		}
		return blk;
	}
	
	public void setBlock(Colunm colunm, int offset){
		DBBlock block = this.getBlock(offset);
		
		if(null != block){
			block.setColunm(colunm);
		}
	}
	
	public Colunm getColunm(int offset){
		Colunm colunm = null;;
		DBBlock blk = this.getBlock(offset);
		if(null != blk){
			colunm = blk.getColunm();
		}
		return colunm;
	}
	
	public DBBlock getBlock(int offset){
		DBBlock blk = null;
//		System.out.println("getBlock " + offset + ", " + this.size + ", " + this.pageID);
		if(offset < this.size){			
			blk = new DBBlock(this);
			blk.setOffset(offset);
			blk.setPageID(this.pageID);
		}

		return blk;
	}
}

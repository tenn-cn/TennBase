package tenndb.data;


public class DBBlock {
	
	//head
	protected int pageID;
	
	protected int offset;
	
	//body
	protected PageBuffer page = null;
	
	public DBBlock(PageBuffer page){
		this.page = page;
	}
	
	public int getTableID(){
		int tableID = 0;
		if(null != this.page){
			tableID = this.page.getTableID();
		}
		return tableID;
	}
	
	public String getTableName(){
		String tableName = null;
		
		if(null != this.page){
			tableName = this.page.getTableName();
		}
		
		return tableName;
	}
	
	public byte[] getBuff(){
		byte[] buff = null;
		
		synchronized(this.page){
			this.page.buffer.rewind();
			this.page.buffer.position(DBPage.HEAD_SIZE + this.offset);
			int len = this.page.buffer.getInt();
			
			if(len > 0){
				buff = new byte[len];
				this.page.buffer.get(buff, 0, len);
			}
		}
		
		return buff;
	}
	
	public String getVar(){
		String var = null;
		byte[] array = null;
		synchronized(this.page){
			this.page.buffer.rewind();
			this.page.buffer.position(DBPage.HEAD_SIZE + this.offset);
	//		this.key = this.page.buffer.getInt();
			int len = this.page.buffer.getInt();
//			System.out.println("len = " + len);
//			len = 10;
			if(len > 0){
				array = new byte[len];
				this.page.buffer.get(array, 0, array.length);
			}

		}
		
		if(null != array)
			var = new String(array);
		
		return var;
	}
	
	public void setVar(String var){
		if(null != var && var.length() > 0){
			byte[] buff = var.getBytes();
			this.setVar(buff);
		}
	}
	
	public void setVar(byte[] var){

		if(null != var && var.length > 0){
			
			synchronized(this.page){
				this.page.buffer.rewind();
				this.page.buffer.position(DBPage.HEAD_SIZE + this.offset);
	//			this.page.buffer.putInt(key);			
				int len = var.length;
				this.page.buffer.putInt(len);
				this.page.buffer.put(var, 0, len);
			}
		}
	}

	public int getPageID() {
		return pageID;
	}

	public void setPageID(int pageID) {
		this.pageID = pageID;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}


	
}

package tenndb.data;

import java.util.ArrayList;
import java.util.List;

import tenndb.common.ByteUtil;


public class DBBlock {
	
	//head
	protected int pageID;
	
	protected int offset;
	
	//body
	protected PageBuffer page = null;
	
	public static final int HEAD_SIZE = ByteUtil.INT_SIZE + ByteUtil.SHORT_SIZE + ByteUtil.SHORT_SIZE; 
	
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
	
	public Colunm getColunm(){
		Colunm colunm = null; 
		List<Filed> fileds = new ArrayList<Filed>();
		int version = 0;
		int length  = 0;
		int key     = 0;
		synchronized(this.page){
			this.page.buffer.rewind();
			this.page.buffer.position(DBPage.HEAD_SIZE + this.offset);
			
			key = this.page.buffer.getInt();

			version = this.page.buffer.getShort();
			if(version < 0){
				version += ByteUtil.SHORT_MAX_VALUE;
			}
			
			length = this.page.buffer.getShort();
			if(length < 0){
				length += ByteUtil.SHORT_MAX_VALUE;
			}
						
			int total = length;
			int index = 0;
			while(total > 0){
				int len = this.page.buffer.getShort();
				if(len < 0){
					len += ByteUtil.SHORT_MAX_VALUE;
				}
				index++;
				if(len > 0){
					byte[] buff = new byte[len];
					this.page.buffer.get(buff);
					String value = new String(buff);
					Filed filed = new Filed("filed_" + index, value);
					fileds.add(filed);
				}
				
				total -= (ByteUtil.SHORT_SIZE + len);
			}
			
		}
		colunm = new Colunm(key, version, fileds); 
		colunm.len = length;
		return colunm;
	}
	
	public void setColunm(Colunm colunm){
		if(null != colunm ){
			this.setVar(colunm.key, colunm.version, colunm.len, colunm.fileds);
		}
	}
	
	public void setVar(int key, int version, int len, List<Filed> fileds){

		if(null != fileds && version >= 0){
			
			synchronized(this.page){
				this.page.buffer.rewind();
				this.page.buffer.position(DBPage.HEAD_SIZE + this.offset);
				//dword key
				//word len
				//word version
				//word filed1_len
				//word filed1_buff
				//word filed2_len
				//word filed2_buff
				this.page.buffer.putInt(key);
				
				byte[] vers = ByteUtil.shortToByte2_big(version);				
				this.page.buffer.put(vers);

				byte[] lens = ByteUtil.shortToByte2_big(len);	
				this.page.buffer.put(lens);
				
				int total = 0;
				for(int i = 0; i < fileds.size(); ++i){
					Filed filed = fileds.get(i);
					if(null != filed.value){
						byte[] buff = filed.value.getBytes();
						if(null != buff && buff.length > 0){
							total += (ByteUtil.SHORT_SIZE + buff.length);							
							if(total <= ByteUtil.SHORT_MAX_VALUE){
								
								byte[] itemsize = ByteUtil.shortToByte2_big(buff.length);
								
								this.page.buffer.put(itemsize,  0, itemsize.length);								
								this.page.buffer.put(buff, 0, buff.length);	
							}else{
								total -= (ByteUtil.SHORT_SIZE + buff.length);
								break;
							}								
						}						
					}
				}
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

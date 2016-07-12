package tenndb.data;

import java.util.ArrayList;
import java.util.List;

import tenndb.common.ByteUtil;

public class Colunm {

	protected String      key;
	
	protected int         hashCode;
	protected int         len;
	protected int         version;
	protected List<Filed> fileds;

	public Colunm(String key, int version) {
		super();		
		this.key      = key;
		this.hashCode = hashCode(key);
		this.version  = version;
		this.fileds   = new ArrayList<Filed>();
		this.len      = 0;
		
		this.addFiled(new Filed("key", key));
	}
	

	
	public static final int hashCode(String value){
		int hashCode = 0;
		if(null != value && value.length() > 0){
			hashCode = value.hashCode();
		}
		return hashCode;
	}
	
	public int getLen(){
		return len;
	}

	public int getVersion() {
		return version;
	}

	public int getHashCode(){
		return this.hashCode;
	}
	
	public String getKey() {
		if(null == this.key && this.fileds.size() > 0){
			this.key = this.fileds.get(0).getValue();
		}
		return key;
	}
	
	public void addFiled(Filed filed){
		if(null != filed){
			this.len += ByteUtil.SHORT_SIZE;
			String value = filed.getValue();
			if(null != value && value.length() > 0){
				this.len += value.length();				
			}
			fileds.add(filed);
		}
	}

	public final List<Filed> getFileds() {
		return fileds;
	}
}

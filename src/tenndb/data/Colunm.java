package tenndb.data;

import java.util.ArrayList;
import java.util.List;

import tenndb.common.ByteUtil;

public class Colunm {

	protected int         key;
	protected int         len;
	protected int         version;
	protected List<Filed> fileds;
	
	public Colunm(int key, int version, List<Filed> fileds) {
		super();		
		this.key     = key;
		this.version = version;
		this.fileds  = fileds;
		this.len     = 0;
	}

	public Colunm(int key, int version) {
		super();		
		this.key     = key;
		this.version = version;
		this.fileds  = new ArrayList<Filed>();
		this.len     = 0;
	}
	
	public int getLen(){
		return len;
	}

	public int getVersion() {
		return version;
	}

	public int getKey() {
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

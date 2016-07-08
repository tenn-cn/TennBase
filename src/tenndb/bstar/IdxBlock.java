package tenndb.bstar;

import java.nio.ByteBuffer;

import tenndb.common.SystemTime;


public class IdxBlock {

	protected int tabId;
	protected int idxPos;
	protected int idxPageID;
	protected int idxOffset;
	
	
	
	protected int key;
	
	protected int pageID;
	
	protected int offset;
	
	protected int time;
	
	protected byte tag;
	
	protected boolean drity;
	//
	protected byte newTag;
	
	protected int tid;
	
	protected IdxBlock old;
	
	public static final byte INVALID = (byte)0;
	public static final byte VALID   = (byte)1;
	
	public int getTabId() {
		return tabId;
	}

	public void setTabId(int tabId) {
		this.tabId = tabId;
	}

	public int getIdxPos() {
		return idxPos;
	}

	public void setIdxPos(int idxPos) {
		this.idxPos = idxPos;
	}

	public int getIdxPageID() {
		return idxPageID;
	}

	public void setIdxPageID(int idxPageID) {
		this.idxPageID = idxPageID;
	}

	public int getIdxOffset() {
		return idxOffset;
	}

	public void setIdxOffset(int idxOffset) {
		this.idxOffset = idxOffset;
	}

	public static void toByteBuffer(IdxBlock blk, ByteBuffer buff){
		if(null != blk){

			buff.putInt(blk.key);
			buff.putInt(blk.pageID);
			buff.putInt(blk.offset);
			buff.putInt(blk.time);
			buff.put(blk.tag);
		}
	}
	
	public IdxBlock(int key) {
		super();
		this.idxPos      = 0;
		this.idxPageID   = 0;
		this.idxOffset   = 0;
		
		this.key         = key;
		this.pageID      = 0;
		this.offset      = 0;
		this.time        = SystemTime.getSystemTime().currentTime();
		this.drity       = false;
		this.tid         = 0;
		this.tag         = VALID;
		this.newTag      = VALID;
		this.old         = null;
	}
	
	public IdxBlock copyData(){
		IdxBlock blk = new IdxBlock(this.key);
		blk.pageID   = this.pageID;
		blk.offset   = this.offset;
		blk.time     = this.time;
		return blk;
	}

	public int getTime(){
		return this.time;
	}
	
	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public byte getTag() {
		return tag;
	}

	public void setTag(byte tag) {
		this.tag = tag;
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

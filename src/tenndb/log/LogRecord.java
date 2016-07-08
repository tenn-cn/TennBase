package tenndb.log;

public class LogRecord {

	//tid 4
	//type 1
	//state      1
	//new.key    4
	//tabId      4
	//new.pageid 4
	//new.offset 2
	//new.tag    1
	//old.pageid 4 
	//old.offset 2
	//old.tag    1
	//prelog.pos 4
	
	protected int    tid;
	protected byte   type;
	protected byte   state;
	protected int    key;
	protected int    tabId;
	protected int    newPid;
	protected short  newOffset;
	protected byte   newTag;
	protected int    oldPid;
	protected short  oldOffset;
	protected byte   oldTag;
	protected int    prePos;
	
	public LogRecord(int tid, byte state, byte type, int key, int tabId, 
					 int newPid, short newOffset, byte newTag, 
					 int oldPid, short oldOffset, byte oldTag) {
		super();
		this.tid       = tid;
		this.type      = type;
		this.state     = state;
		this.key       = key;
		this.tabId     = tabId;
		this.newPid    = newPid;
		this.newOffset = newOffset;
		this.newTag    = newTag;
		this.oldPid    = oldPid;
		this.oldOffset = oldOffset;
		this.oldTag    = oldTag;
	}

	public int getTid() {
		return tid;
	}

	public byte getType() {
		return type;
	}

	public byte getState() {
		return state;
	}

	public int getKey() {
		return key;
	}

	public int getTabId() {
		return tabId;
	}

	public int getNewPid() {
		return newPid;
	}

	public short getNewOffset() {
		return newOffset;
	}

	public byte getNewTag() {
		return newTag;
	}

	public int getOldPid() {
		return oldPid;
	}

	public short getOldOffset() {
		return oldOffset;
	}

	public byte getOldTag() {
		return oldTag;
	}

	public int getPrePos() {
		return prePos;
	}	

}

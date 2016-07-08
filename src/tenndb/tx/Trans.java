package tenndb.tx;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class Trans implements Serializable {

	private static final long serialVersionUID = -3717435910507148488L;

	protected static AtomicInteger _counter = new AtomicInteger(1);
	
	protected int transID = 0;
	
	protected byte state;
	
	public static final byte INVALID     = 0;
	public static final byte START 	     = 1;
	public static final byte ACTIVE 	 = 2;
	public static final byte TERMINATED  = 3;
	
	public static final byte COMMIT  	 = 4;	
	public static final byte ROLLBACK  	 = 5;
	
	public static final byte REDO        = 6;
	public static final byte UNDO      	 = 7;

//normal
//	                  COMMITED 
// START -> ACTIVE ->          -> TERMINATED
//                    ROLLBACK
//
	
//crashdown
//COMMITED -> REDO
//ROLLBACK -> UNDO
	
	public Trans(){
		this.transID = _counter.getAndIncrement();
		this.state   = ACTIVE;
	}
	
	public int getState() {
		return state;
	}

	public void setState(byte state) {
		this.state = state;
	}

	public int getTransID(){
		return this.transID;
	}
	
	public int hashCode(){
		return this.transID;
	}
}

package tenndb.common;

import java.util.Date;

public class SystemTime {

	protected long baseTime = 0;
	
	protected long baseTick = 0;
	
	protected static volatile SystemTime _inst = null;
	
	protected static final Object _lock = new Object();
	
	private SystemTime(){
		this.baseTime = new Date().getTime();
		this.baseTick = System.currentTimeMillis();
	}
	
	public static SystemTime getSystemTime(){
		
		if(null == _inst){
			synchronized(_lock){
				if(null == _inst){
					_inst = new SystemTime();
				}
			}
		}

		return _inst;
	}
	
	public int currentTime(){
		return (int) (this.currentTimeMillis()/1000L);
	}

	public long currentTimeMillis(){
		long currentTime = 0;
		
		long lastTick = System.currentTimeMillis();
		
		if((lastTick - this.baseTick) > 10*1000L){
			synchronized(_lock){
				if((lastTick - this.baseTick) > 10*1000L){
//					System.out.println("+++++++++++++++++++++++++++++++++++++++getCurrentTime");
					this.baseTime = new Date().getTime();
					this.baseTick = System.currentTimeMillis();
					lastTick = this.baseTick;
				}
			}
		}
		
		currentTime = this.baseTime + (lastTick - this.baseTick); 
		
		return currentTime;
	}
	
}

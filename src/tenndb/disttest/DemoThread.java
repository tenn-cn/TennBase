package tenndb.disttest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import tenndb.common.SystemTime;
import tenndb.dist.DistMgr;

public class DemoThread extends Thread {

	protected int devID;
	protected DistMgr distMgr;
	
	public DemoThread(int devID, DistMgr distMgr) {
		super();
		this.devID   = devID;
		this.distMgr = distMgr;
	}
	protected static int SIZE = 100;
	
	public void run(){
		int time = SystemTime.getSystemTime().currentTime();
		List<byte[]> list = new ArrayList<byte[]>();
		ByteBuffer buffer = ByteBuffer.allocate(25);
		
		for(int i = 0; i < SIZE; ++i){
			byte[] array = new byte[25];
			list.add(array);
		}
		
		while(true){
			try{
				int lng  = 1200000000;
				int lat  =  300000000;
				
				for(int i = 0; i < SIZE; ++i){
					time++;
					String strDevID   = String.valueOf(this.devID);
					byte[] devIDArray = new byte[10];
					byte[] temp       = strDevID.getBytes();
					System.arraycopy(temp, 0, devIDArray, 0, 10);
					
					buffer.clear();
					buffer.rewind();
					buffer.put(DistMgr.TAG_DATA);
					buffer.put(devIDArray); 	//10
					buffer.putInt(time);    	//4
					buffer.putShort((short) 8);	//2
					buffer.putInt(lng + i); 	//4
					buffer.putInt(lat + i); 	//4
					
					byte[] array = list.get(i);
					System.arraycopy(buffer.array(), 0, array, 0, array.length);
				}
				
				this.distMgr.write(list);
				
			}catch(Exception e){
				System.out.println(e);
			}finally{
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

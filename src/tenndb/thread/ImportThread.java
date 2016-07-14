package tenndb.thread;

import java.io.IOException;
import java.nio.ByteBuffer;

import tenndb.base.Cell;
import tenndb.common.ByteUtil;
import tenndb.common.FileMgr;
import tenndb.dist.DistPage;
import tenndb.dist.DistPageMgr;
import tenndb.route.RouteMgr;

public class ImportThread extends Thread {

	protected FileMgr   fileMgr;
	protected RouteMgr  routeMgr;
	
	protected final ByteBuffer buffer;
	protected final byte[] buff;

	public ImportThread(FileMgr fileMgr, RouteMgr routeMgr) {
		super();
		this.fileMgr     = fileMgr;
		this.routeMgr    = routeMgr;
		this.buffer      = ByteBuffer.allocate(DistPage.PAGE_SIZE);
		this.buff        = new byte[ByteUtil.SHORT_MAX_VALUE];
	}
	
	public void run(){
		this.reflush();
		
		while(true){
			try{
		
			}catch(Exception e){
				System.out.println(e);
			}finally{
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	
	public void reflush(){
		String[] files = this.fileMgr.listFiles();
		if(null != files && files.length > 0){
			
			for(String fileName : files){
				
				if(fileName.startsWith(DistPageMgr.PREFIX_TEMP)){
					try {
						this.buffer.clear();
						this.fileMgr.readBuffer(fileName, this.buffer.array(), 0);
						
						byte[] devArray = new byte[10];
						this.buffer.get(devArray);
						int date  = this.buffer.getInt();
						int len = this.buffer.getShort();
						int time = this.buffer.getInt();
						
						if(len < 0){
							len += ByteUtil.SHORT_MAX_VALUE;
						}
						if(len > 0){
							this.buffer.get(buff, 0, len);							
						}
						String strDate = String.valueOf(date);
						String strDevID = new String(devArray);
						
						Cell cell = this.routeMgr.pinLevel2(strDate, strDevID);
						if(null != cell){
							cell.insert(time, this.buff, 0, len);
						}
					} catch (Exception e) {
						System.out.println(e);
					}
				}
			}
		}
	}
}

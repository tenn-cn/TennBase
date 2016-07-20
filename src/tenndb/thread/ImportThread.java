package tenndb.thread;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import tenndb.base.Cell;
import tenndb.common.ByteUtil;
import tenndb.common.DateFormatUtil;
import tenndb.common.FileMgr;
import tenndb.dist.DistPage;
import tenndb.dist.DistMgr;
import tenndb.route.RouteMgr;

public class ImportThread extends Thread {

	protected FileMgr   fileMgr;
	protected RouteMgr  routeMgr;
	
	protected final ByteBuffer buffer;
	protected final byte[] buff;

	private static final ThreadLocal<SimpleDateFormat> LOG_DATE_FORMAT =
		DateFormatUtil.threadLocalDateFormat("yyyyMMdd");
	
	public ImportThread(FileMgr fileMgr, RouteMgr routeMgr) {
		super();
		this.fileMgr     = fileMgr;
		this.routeMgr    = routeMgr;
		this.buffer      = ByteBuffer.allocate(DistPage.PAGE_SIZE);
		this.buff        = new byte[DistPage.PAGE_SIZE];
	}
	
	public void run(){
		this.reflush(DistMgr.PREFIX_TEMP + DistMgr.PREFIX_QUEUE);
		
		while(true)
		{
			try{
				this.reflush(DistMgr.PREFIX_QUEUE);
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
	
	public void reflush(String prefix){
		if(null != prefix && prefix.length() > 0){
			String[] files = this.fileMgr.listFiles();
			if(null != files && files.length > 0){
				
				SimpleDateFormat df = LOG_DATE_FORMAT.get();
				
				for(String fileName : files){
					
					if(fileName.startsWith(prefix)){
//						System.out.println("fileName = " + fileName);
						try {
							this.buffer.clear();
							this.fileMgr.readBuffer(fileName, this.buffer.array(), 0);
							
							/*
			buffer.put(devIDArray); //10
			buffer.putInt(time);    //4
			buffer.putShort((short) 8);//2
			buffer.putInt(lng + i); //4
			buffer.putInt(lat + i); //4
							 */
							this.buffer.rewind();
							while(buffer.remaining() > 0){
								byte tag = this.buffer.get();
								
								if(DistMgr.TAG_NULL == tag){
									break;
								}
								
								byte[] devArray = new byte[10];
																
								this.buffer.get(devArray);
								
								String strDevID = new String(devArray);
																
								int time = this.buffer.getInt();
								int len  = this.buffer.getShort();
//								System.out.println("len = " + len);
								
								if(len < 0){
									len += ByteUtil.SHORT_MAX_VALUE;
								}
								if(len > 0){
									this.buffer.get(buff, 0, len);							
								}
								
								String strDate  = df.format(new Date(time * 1000L));						
//								System.out.println("pinLevel2: strDate = " + strDate + ", strDevID = " + strDevID);
								
								Cell cell = this.routeMgr.pinLevel2(strDate, strDevID);
								if(null != cell){
//									System.out.println("insert: time = " + time + ", len = " + len);
									cell.insert(time, this.buff, 0, len);
								}
							}

						} catch (Exception e) {
							System.out.println(e);
						}
						
//						String newFileName = DistMgr.PREFIX_DELETE + fileName;
						
						this.fileMgr.delete(fileName);
						
//						break;
					}
					
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

				}
			}
		}		
	}
}
 
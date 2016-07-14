package tenndb.thread;

import java.util.List;

import tenndb.dist.DistPage;
import tenndb.dist.DistMgr;

public class DistThread extends Thread {

	protected DistMgr pageMgr = null;
	
	public DistThread(DistMgr pageMgr) {
		super();
		this.pageMgr = pageMgr;
	}

	public void run(){
		
		while(true){
			try{
				if(null != this.pageMgr){
					List<DistPage> list = this.pageMgr.getAndClearUsedList();
					if(null != list && list.size() > 0){
						System.out.println(list.size());
						for(DistPage page : list){
							String fileName = this.pageMgr.newFileName();
							System.out.println("fileName = " + fileName) ;
							this.pageMgr.flush(page, fileName);
						}
					}					
				}
			}catch(Exception e){}
			finally{
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

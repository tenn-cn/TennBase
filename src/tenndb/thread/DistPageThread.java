package tenndb.thread;

import java.util.List;

import tenndb.dist.DistPage;
import tenndb.dist.DistPageMgr;

public class DistPageThread extends Thread {

	protected DistPageMgr pageMgr = null;
	
	public DistPageThread(DistPageMgr pageMgr) {
		super();
		this.pageMgr = pageMgr;
	}

	public void run(){
		
		while(true){
			try{
				if(null != this.pageMgr){
					List<DistPage> list = this.pageMgr.getAndClearUsedList();
					if(null != list && list.size() > 0){
						for(DistPage page : list){
							String fileName = this.pageMgr.newFileName();
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

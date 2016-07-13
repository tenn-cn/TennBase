package tenndb.thread;

import java.util.Date;

import tenndb.base.Cell;
import tenndb.bstar.RandomInsertBStarTree;
import tenndb.common.SystemTime;

public class FlushHook extends Thread {

	protected Cell cell          = null;
	protected int lastTime = 0;
	public FlushHook(Cell cell) {
		super();
		this.cell = cell;
		this.lastTime = SystemTime.getSystemTime().currentTime();
	}

	public void run(){
		while(true){
			try{
				if(null != this.cell){
					RandomInsertBStarTree tree = (RandomInsertBStarTree) this.cell.getIndex();
					boolean next = false;
					tree.lockWrite();
					next = this.cell.flushNewPages();
					
					int current = SystemTime.getSystemTime().currentTime();
					
					if(next || (current > (this.lastTime + 60))){
						long t1 = System.currentTimeMillis();
						this.cell.flush();
						long t2 = System.currentTimeMillis();
						System.out.println("flush, cost = " + (t2 - t1) + ", " + new Date());
						this.lastTime = current;
					}
					
					tree.unLockWrite();
				}
			}catch(Exception e){
				System.out.println(e);
			}
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

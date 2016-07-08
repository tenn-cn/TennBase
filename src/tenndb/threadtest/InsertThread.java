package tenndb.threadtest;

import tenndb.RefVar;
import tenndb.base.Cell;

public class InsertThread extends Thread {

	protected Cell cell;
	
	public InsertThread(Cell cell) {
		super();
		this.cell = cell;
	}

	public void run(){
				
		RefVar r = new RefVar();
		
//		while(true)
		{
			for(int i = 1; i <= 100000; ++i){
				
				String str = i + "_helloworld_";
				this.cell.insert(i, str, r);
			}
		}
	}
}

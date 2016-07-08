package tenndb.threadtest;

import tenndb.base.Cell;

public class InsertThread extends Thread {

	protected Cell cell;
	
	public InsertThread(Cell cell) {
		super();
		this.cell = cell;
	}

	public void run(){
				

//		while(true)
		{
			for(int i = 1; i <= 100000; ++i){
				
				String str = i + "_helloworld_";
				this.cell.insert(i, str);
			}
		}
	}
}

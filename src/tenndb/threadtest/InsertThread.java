package tenndb.threadtest;

import tenndb.base.Cell;
import tenndb.data.Colunm;
import tenndb.data.Filed;

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
				Colunm colunm = new Colunm(i, 1);
				colunm.addFiled(new Filed("var1", str));
				this.cell.insert(i, colunm);
			}
		}
	}
}

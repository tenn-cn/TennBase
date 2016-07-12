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
			for(int i = 1; i <= 1000; ++i){
				
				String key = i + "_helloworld_";
				
				Colunm colunm = new Colunm(key, 1);
				colunm.addFiled(new Filed("var1", key + 1));
/*				colunm.addFiled(new Filed("var2", key + 2));
				colunm.addFiled(new Filed("var3", key + 3));
				colunm.addFiled(new Filed("var4", key + 4));
				colunm.addFiled(new Filed("var5", key + 5));
				colunm.addFiled(new Filed("var6", key + 6));*/
				this.cell.insert(i, colunm);
			}
		}
	}
}

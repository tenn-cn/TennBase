package tenndb.test;

import java.util.List;

import tenndb.base.Cell;
import tenndb.common.FileMgr;
import tenndb.data.Colunm;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;

public class TestCell {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		TransMgr trasMgr = new TransMgr();
		
		FileMgr fileMgr = new FileMgr("J:\\tennbase\\");
		String dbName = "stu";
		int dbID = 123;
		Cell cell = new Cell(dbName, dbID, fileMgr, trasMgr, null);
		
		cell.init();
		/*	
		for(int i = 1000; i < 2000; ++i){
			int key = i;
			String var = i + "_hellowrold";
			cell.insert(key, var);
		}
		
		for(int i = 2000; i < 3000; ++i){
			int key = i;
			String var = i + "_hellowrold";
			cell.insert(key, var);
		}
	
		for(int i = 3000; i < 4000; ++i){
			int key = i;
			String var = i + "_hellowrold";
			cell.insert(key, var);
		}
			*/
		Trans tid = new Trans();	
/*		for(int i = 0; i < 100; ++i){
			int key = i;
			String var = i + "_hellowrold";
			cell.insert(key, var, tid);
			cell.commit(key, tid);
		}
			*/
		
	//	cell.delete(51);
	//	TransID tid = new TransID();	

 		for(int i = 1; i <= 100; i+=10){
			Colunm var = cell.search(i, tid);
			System.out.println("key = " + i + ", var = " + var.getKey());
		}
 		
 		
 		int n1 = cell.count(33, 88);
		System.out.println("count = " + n1);
		
		

		System.out.println("++++++++++++++++++++++++++++");
		
		List<Colunm> list2 = cell.range(8888, 8988, false);
		
		for(int i = 0; i < list2.size(); ++i){
			System.out.println("range2 " + list2.get(i).getKey());
		}
		
		cell.printTreeNext();
		
	//	reactor.print();
/*		try {
			Thread.sleep(1000*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		cell.flush();
	}

}

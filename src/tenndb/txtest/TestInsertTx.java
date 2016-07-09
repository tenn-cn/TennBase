package tenndb.txtest;

import tenndb.base.Catalog;
import tenndb.base.Cell;
import tenndb.base.TennBase;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;

public class TestInsertTx {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Catalog catalog = TennBase.getCatalog();
		String dbStudent = "student";
		int dbStudentID = 1;
		catalog.addCell(dbStudent, dbStudentID);
		Cell cellStu = catalog.getCell(dbStudent);
		
/*		Trans trans = catalog.beginTrans();
		for(int i = 1; i < 100; ++i){
			String str = i + "_helloworld_";			
			try {
				cellStu.insert(i, str, trans);
			} catch (AbortTransException e) {
				System.out.println(e);
				e.printStackTrace();
			}
		}
		
	//	catalog.rollback(trans);
		catalog.commit(trans);*/
		
		for(int i = 1; i <= 100000; ++i){
			String str = cellStu.search(i, null);
			if(null != str){
				System.out.println("search" + i + " : " + str);
			}
		}
	}
}

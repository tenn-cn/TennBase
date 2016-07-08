package tenndb.threadtest;

import java.util.ArrayList;
import java.util.List;

import tenndb.base.Catalog;
import tenndb.base.Cell;
import tenndb.base.TennBase;

public class TestTennBase {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Catalog catalog = TennBase.getCatalog();
		String dbStudent = "student";
		int dbStudentID = 1;
		catalog.addCell(dbStudent, dbStudentID);
		Cell cellStu = catalog.getCell(dbStudent);
		
		List<InsertThread> list = new ArrayList<InsertThread>();
		
		for(int i = 0; i < 10; ++i){
			InsertThread thread = new InsertThread(cellStu);
			list.add(thread);
		}
		
		for(InsertThread thread : list){
			thread.start();
		}
	}

}

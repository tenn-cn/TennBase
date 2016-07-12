package tenndb.txtest;

import tenndb.base.Catalog;
import tenndb.base.Cell;
import tenndb.base.TennBase;
import tenndb.data.Colunm;
import tenndb.data.Filed;
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
		
		Trans trans1 = catalog.beginTrans();
		Trans trans2 = catalog.beginTrans();
/*		
		for(int i = 1; i < 100; i+=2){
			String str1 = i + "_helloworld1_";
			String str2 = i+1 + "_helloworld2_";
			try {
				cellStu.insert(i, str1, trans1);
			} catch (AbortTransException e) {
				System.out.println(e);
				e.printStackTrace();
			}
			
			try {
				cellStu.insert(i+1, str2, trans2);
			} catch (AbortTransException e) {
				System.out.println(e);
				e.printStackTrace();
			}
		}
		
		{
			String str = 99 + "_helloworld2_";
			try {
				cellStu.insert(99, str, trans2);
			} catch (AbortTransException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		{
			String str = 98 + "_helloworld2_";
			try {
				cellStu.insert(98, str, trans1);
			} catch (AbortTransException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
		
		
		
		{
			int key = 1;
			String str = key + "_helloworld_1";
			try {
				Colunm colunm = new Colunm(str, 1);
				colunm.addFiled(new Filed("var1", str + 1));
				
				cellStu.insert(key, colunm, trans1);
			} catch (AbortTransException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				catalog.rollback(trans1);
			}
		}
		
		
		{
			int key = 2;
			String str = key + "_helloworld_2";
			try {
				Colunm colunm = new Colunm(str, 1);
				colunm.addFiled(new Filed("var1", str + 1));
				
				cellStu.insert(key, colunm, trans2);
			} catch (AbortTransException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				catalog.rollback(trans2);
			}
		}
		
		
		{
			int key = 1;
			String str = key + "_helloworld_2";
			try {
				Colunm colunm = new Colunm(str, 1);
				colunm.addFiled(new Filed("var1", str + 1));
				
				cellStu.insert(key, colunm, trans2);
			} catch (AbortTransException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				catalog.rollback(trans2);
			}
		}
		
		
		{
			int key = 2;
			String str = key + "_helloworld_1";
			try {
				Colunm colunm = new Colunm(str, 1);
				colunm.addFiled(new Filed("var1", str + 1));
				
				cellStu.insert(key, colunm, trans1);
			} catch (AbortTransException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				catalog.rollback(trans1);
			}
		}
		
		

		
//		catalog.rollback(trans);
//		catalog.commit(trans1);
//		catalog.commit(trans2);
		
		for(int i = 1; i <= 100000; ++i){
			Colunm colunm = cellStu.search(i, null);
			if(null != colunm){
				System.out.println("search" + i + " : " + colunm.getKey());
			}
		}
	}
}

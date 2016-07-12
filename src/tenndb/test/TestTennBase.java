package tenndb.test;


import tenndb.base.Catalog;
import tenndb.base.Cell;
import tenndb.base.TennBase;
import tenndb.data.Colunm;
import tenndb.data.Filed;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;

public class TestTennBase {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Catalog catalog = TennBase.getCatalog();
		
		String dbStudent    = "student";
		String dbDepartment = "department";
		int dbStudentID = 1;
		int dbDepartmentID = 2;
		if(null != catalog){
			
		//	tid = null;

			catalog.addCell(dbStudent, dbStudentID);
//			catalog.addCell(dbDepartment);
			Cell cellStu = catalog.getCell(dbStudent);
			
			long t1 = System.currentTimeMillis();
				
	//		cellStu.print();
			for(int i = 1; i <= 1; ++i){
				
				String str = i + "_helloworld_";
				
				Colunm colunm = new Colunm(i, 1);
				colunm.addFiled(new Filed("var1", str + 1));
				colunm.addFiled(new Filed("var2", str + 2));
				colunm.addFiled(new Filed("var3", str + 3));
				colunm.addFiled(new Filed("var4", str + 4));
				colunm.addFiled(new Filed("var5", str + 5));
				colunm.addFiled(new Filed("var6", str + 6));
				
				cellStu.insert(i, colunm);
			}
			
			{
				
				int num = 0;
				for(int i = 1; i <= 1; ++i){
					Colunm colunm = cellStu.search(i);
					if(null != colunm)
					{
						System.out.println("search" + i + " : " + colunm.getKey() + ", " + colunm.getVersion() + ", " + colunm.getLen());
						for(int t = 0; t < colunm.getFileds().size(); ++t){
							Filed filed = colunm.getFileds().get(t);
							System.out.println("       " + filed.getName() + ", " + filed.getValue());
						}
					}
					else{
						++num;
						System.out.println("+++++++++++++++++++++++++++++++++ miss " + i);
					}
				}
				System.out.println(" miss " + num);
			}
			
			{
				Colunm str = cellStu.search(99477, null);
				System.out.println("search " + str);
			}
			
/*			Trans trans = catalog.beginTrans();
			for(int i = 1; i <= 10000; ++i){
				
				String str = i + "_helloworld";
				try {
					cellStu.insert(i, str, trans);
				} catch (AbortTransException e) {
					System.out.println(e);
				}
				
			}*/
			long t2 = System.currentTimeMillis();
		
	//		catalog.commit(trans);
		
			int count = 0;
/*			for(int i = 100001; i <= 200000; ++i){
				String str = cellStu.search(i);
				if(null == str || str.length() == 0){
					count++;
				}
			}*/
			
			long t3 = System.currentTimeMillis();
			
//			Cell cellDep = catalog.getCell(dbDepartment);
			
			//0123456789
			//abcdefghij
			//kmnpqrstuv
			//wsyz_
			//35

//			cellStu.print();
	
/*			{
				TransID tid = catalog.getTransID();
				System.out.println("transID = " + tid.getTransID());
		//		tid = null;
				for(int i = 1; i <= 100; ++i){
					boolean b = cellStu.insert(i, i +"_student_3",    tid);
					System.out.println("insert.x " + b);
					TransID tid2 = catalog.getTransID();
					boolean b2 = cellStu.insert(i, i +"_student_4",    tid2);
					System.out.println("insert.y " + b2);
					catalog.commit(tid2);
					
				}
				
				catalog.commit(tid);
			}*/
			
/*			for(int t = 0; t < 100; ++t)
			{
				TransID tid = catalog.beginTrans();
				System.out.println("transID = " + tid.getTransID());
		//		tid = null;
				for(int i = 1; i <= 1000; ++i){
					boolean b = cellStu.insert(t*100 + i, (t*100 + i) +"_student_" + (t*100 + i),    tid);					
				}
				
				catalog.commit(tid);
			}*/
			
/*			{
				TransID tid = catalog.getTransID();
				System.out.println("transID = " + tid.getTransID());
		//		tid = null;
				for(int i = 1; i <= 10; ++i){
					TransID tid2 = catalog.getTransID();
					cellStu.update(i, i +"_student_2",    tid);
					cellStu.delete(i, tid2);
					catalog.commit(tid2);
				}
				
				catalog.commit(tid);
			}*/
		
/*			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			
			{
				Trans tid = catalog.beginTrans();
				System.out.println("transID = " + tid.getTransID());
				for(int i = 1; i <= 10; ++i){
					String str = cellStu.search(i, tid);
					System.out.println("search" + i + " : " + str);
				}
				catalog.commit(tid);
			}
			
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");*/
			/*
			
			
			{
				TransID tid = catalog.getTransID();
				System.out.println("transID = " + tid.getTransID());
		//		tid = null;
				for(int i = 1; i <= 10; ++i){
					cellStu.insert(i, i +"_student_2",    tid);
				}
				
				catalog.commit(tid);
			}
			
			
			{
				TransID tid = catalog.getTransID();
				System.out.println("transID = " + tid.getTransID());
				for(int i = 1; i <= 10; ++i){
					String str = cellStu.search(i, tid);
					System.out.println("search" + i + " : " + str);
				}
				catalog.commit(tid);
			}
	*/
//			catalog.flush();
			
//			cellStu.print();
			
//			cellDep.print();
		}
		
	}

}

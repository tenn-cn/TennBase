package tenndb.test;

import tenndb.bstar.IdxBlock;
import tenndb.common.FileMgr;
import tenndb.data.DBBlock;
import tenndb.data.DBPageMgr;
import tenndb.index.IBTree;
import tenndb.index.IndexMgr;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;


public class TestDBPageMgr {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		FileMgr fileMgr = new FileMgr("J:\\tennbase\\");

		String dbName = "stu";		
		
		TransMgr trasMgr = new TransMgr();
		IndexMgr indexMgr   = new IndexMgr(dbName, fileMgr, trasMgr);
		DBPageMgr dbPageMgr = new DBPageMgr(dbName, fileMgr);
		dbPageMgr.load();
		indexMgr.load();
		
		IBTree tree = indexMgr.getBTree();
		System.out.println(tree.toString());
		tree.printNext();
//		Trans tid = new Trans();
		for(int i = 0; i < 100; ++i){
			int key = i;
			String str = i + "_hellowrold";
			byte[] var = str.getBytes();
			
/*			DBBlock dbblk = dbPageMgr.nextDBBlock(var);
			if(null != dbblk){


				
				dbblk.setVar(var);
				System.out.println("pageID = " + dbblk.getPageID() + ", offset = " + dbblk.getOffset());
				IdxBlock idxblk = new IdxBlock(key);
				idxblk.setPageID(dbblk.getPageID());
				idxblk.setOffset(dbblk.getOffset());
				try {
					tree.insert(key, idxblk, null, null);
				} catch (AbortTransException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
		}

 		for(int i = 1; i < 100; i*=10){
			int key = i;
			IdxBlock idxblk = tree.search(key, null);
			System.out.println(idxblk.getPageID() + ", " + idxblk.getOffset());
			DBBlock dbblk = dbPageMgr.getDBBlock(idxblk.getPageID(), idxblk.getOffset());
			System.out.println(key + ", " + dbblk.getColunm().getKey());
		}
 		
		indexMgr.flush();
		dbPageMgr.flush();
	}

}

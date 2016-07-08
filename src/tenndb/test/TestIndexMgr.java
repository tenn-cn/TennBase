package tenndb.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import tenndb.bstar.IdxBlock;
import tenndb.common.FileMgr;
import tenndb.index.IBTree;
import tenndb.index.IndexMgr;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;

public class TestIndexMgr {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
		TransMgr trasMgr = new TransMgr();
		FileMgr fileMgr = new FileMgr("J:\\tennbase\\");
		IndexMgr indexMgr = new IndexMgr("stu", fileMgr, trasMgr);
		indexMgr.load();
		IBTree tree = indexMgr.getBTree();
//		Trans tid = new Trans();
		for(int i = 0; i < 100; ++i){
			int key = i;
			IdxBlock blk = new IdxBlock(key);
			blk.setPageID(i);
			blk.setOffset(1);
/*			try {
				tree.insert(key, blk, null, null);
			} catch (AbortTransException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		}
		
		List<ByteBuffer> bufferList = new ArrayList<ByteBuffer>();
/*
		try{
			tree.toByteBuffer(bufferList);
		}catch(Exception e)
		{
			System.out.println(e);
		}*/
		
		System.out.println("toByteBuffer, bufferList.len = " + bufferList.size());
		
		tree.printNext();
		
//  	indexMgr.flush();
		
	}

}

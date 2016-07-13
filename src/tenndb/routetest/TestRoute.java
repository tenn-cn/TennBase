package tenndb.routetest;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import tenndb.base.Cell;
import tenndb.common.SystemTime;
import tenndb.data.Colunm;
import tenndb.data.Filed;
import tenndb.route.RouteMgr;

public class TestRoute {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		RouteMgr mgr = new RouteMgr("J:\\tennbase");
		mgr.init();
		
//		Cell c160713 = mgr.pinLevel1("160713");
		String date = "160713";
		Map<String, Cell> map = new Hashtable<String, Cell>();
		List<Cell> list = new ArrayList<Cell>();
		
		for(int i = 0; i < 100; ++i)
		{
			long dev = 1607130000 + i;
			String level2 = String.valueOf(dev);
			Cell cell2 = mgr.pinLevel2(date, level2);
			if(null != cell2){
				map.put(cell2.getDbName(), cell2);
				list.add(cell2);
			}
		}

		int when = SystemTime.getSystemTime().currentTime();
		for(int i = 1; i < 1000; ++i){
			String key = String.valueOf(when + i);
			Colunm colunm = new Colunm(key, 1);
			colunm.addFiled(new Filed("var1", key + 1));
			
			for(int t = 0; t < list.size(); ++t){
				Cell cell = list.get(t);
				cell.insert(colunm.getHashCode(), colunm);
			}
		}


	}

}

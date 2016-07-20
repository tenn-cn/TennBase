package tenndb.disttest;

import java.util.ArrayList;
import java.util.List;

import tenndb.dist.DistMgr;
import tenndb.route.RouteMgr;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		DistMgr distMgr = new DistMgr("J:\\tennbase");
		distMgr.init();
		
		RouteMgr routeMgr = new RouteMgr("J:\\tennbase");
		routeMgr.init();

		int devID = 1607140000;
		List<DemoThread> list = new ArrayList<DemoThread>();
		
		for(int i = 0; i < 40; ++i){
			DemoThread thread = new DemoThread(devID + i, distMgr);
			list.add(thread);			
		}

		for(DemoThread thread : list){
			thread.start();
		}
	}

}

package tenndb.disttest;

import tenndb.dist.DistMgr;
import tenndb.route.RouteMgr;

public class TestRoute {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		RouteMgr routeMgr = new RouteMgr("J:\\tennbase");
		routeMgr.init();

//		DistMgr distMgr = new DistMgr("J:\\tennbase");
//		distMgr.init();
		
		
	}

}

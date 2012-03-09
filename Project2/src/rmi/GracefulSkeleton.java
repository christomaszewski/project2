package rmi;

/******************************************************************************
 * 
 * Authors: Christopher Tomaszewski (CKT) & Dinesh Palanisamy (DINESHP) 
 * 
 ******************************************************************************/

import java.net.InetSocketAddress;

public class GracefulSkeleton<T> extends Skeleton<T> {
	
	public GracefulSkeleton(Class<T> c, T server, InetSocketAddress address) {
		super(c, server, address);
	}

	@Override
	protected void stopped(Throwable cause){
		synchronized(this){
			this.notify();
		}
	}
	
}

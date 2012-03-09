package common;

/******************************************************************************
 * 
 * Authors: Christopher Tomaszewski (CKT) & Dinesh Palanisamy (DINESHP) 
 * 
 ******************************************************************************/

import java.io.Serializable;
import java.util.Iterator;

//A iterator for iterating on path components, which does not support remove
public class PathIterator implements Iterator<String>, Serializable{
	private Iterator<String> iterator;
	
	public PathIterator(Iterator<String> it){
		this.iterator = it;
	}
	
	
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	@Override
	public String next() {
		return this.iterator.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}

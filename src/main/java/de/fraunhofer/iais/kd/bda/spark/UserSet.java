package de.fraunhofer.iais.kd.bda.spark;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

public class UserSet implements Iterable<String>, Serializable {

	private static final long serialVersionUID = 1561110513166623100L;
	private final HashSet<String> backingSet;

	public UserSet() {
		backingSet = new HashSet<String>();
	}

	public UserSet(String userCsv, String delimiter) {
		this();
		addAll(userCsv, delimiter);
	}

	private void addAll(String userCsv, String delimiter) {
		String userIds[] = userCsv.split(delimiter);
		String userIntal = "user_";
		int num = 1;
		for (String uid : userIds) {
			if (uid.equals("1")) {
				String formatted = String.format("%06d", num);
				String uidmod = userIntal + formatted;
				this.add(uidmod);
			}
			num += 1;

		}
	}
	
	public UserSet addUserSet(UserSet other){
		
		UserSet tmp = new UserSet();
		tmp.backingSet.addAll(other.backingSet);
		return tmp;
		
	}

	
	public void add(String username) {
		backingSet.add(username);
	}
	
	public HashSet<String> get() {
		return backingSet;
	}

	public static void add(UserSet u, String username) {
		u.backingSet.add(username);
	}
	
	public double distanceTo(UserSet other) {
		HashSet<String> union = new HashSet<String>(backingSet);
		
		union.addAll(other.backingSet);
	  
		HashSet<String> intersection = new HashSet<String>(backingSet);
		intersection.retainAll(other.backingSet);
		//System.out.print(intersection.size() +"Ratio"+ union.size()+" ");
		double dist = 1.0 - (double) intersection.size() / union.size();
		return dist;
	}

	@Override
	public UserSetIterator iterator() {
		return new UserSetIterator();
	}

	public final class UserSetIterator implements Iterator<String> {

		private final Iterator<String> iterator;

		public UserSetIterator() {
			iterator = backingSet.iterator();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public String next() {
			return iterator.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}


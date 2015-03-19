import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable {
	private static final int N = 200;
	private int CurrentRecords;
	ArrayList<Object> data;
	int beginningID;
	int endingID;

	public Page(int B) {
		data = new ArrayList<Object>(N);
		CurrentRecords = 0;
		beginningID = B;
		endingID = B+N-1;
	}

	public boolean insert(Object x) {
		if(isFull())
			return false;
		CurrentRecords++;
		data.add(x);
		return true;
		
	}
	public boolean remove(Object x){
		if(data.isEmpty())
			return false;
		CurrentRecords--;
		return data.remove(x);
	}

	public boolean isFull() {
		if (CurrentRecords < N)
			return false;
		else
			return true;
	}

}

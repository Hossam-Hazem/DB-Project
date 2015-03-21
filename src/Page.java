import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

public class Page implements Serializable {
	private  final int N;
	private int CurrentRecords;
	ArrayList<Object> data;
	int beginningID;
	int endingID;

	public Page(int B) throws IOException {
		N =  getPageSize();
		System.out.println(N);;
		data = new ArrayList<Object>(N);
		CurrentRecords = 0;
		beginningID = B;
		endingID = B+N-1;
	}
	
	public int getPageSize() throws IOException  {
		InputStream input = new FileInputStream("DBApp.properties");
		Properties prop = new Properties();
		prop.load(input);
		return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
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

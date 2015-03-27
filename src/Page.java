import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

//metadata.csv
public class Page implements Serializable {
	private int MaximumRowsCountinPage;
	private int rowsCounter;
	private ArrayList<Hashtable<String, String>> records;
	private String pageName;
	
	private static final long serialVersionUID = -6451792610345910852L;

	public Page(String pageName) throws IOException {
		this.MaximumRowsCountinPage = getPageSize();
		records = new ArrayList<Hashtable<String, String>>(
				MaximumRowsCountinPage);
		rowsCounter = 0;
		this.pageName = pageName;

		System.out.println(this.MaximumRowsCountinPage);
	}

	public int getPageSize() throws IOException {
		InputStream input = new FileInputStream("config/DBApp.properties");
		Properties prop = new Properties();
		prop.load(input);
		return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
	}

	public boolean isFull() {
		if (rowsCounter < MaximumRowsCountinPage) {
			return false;
		}
		return true;
	}

	public int getMaximumRowsCountinPage() {
		return MaximumRowsCountinPage;
	}

	public void setMaximumRowsCountinPage(int maximumRowsCountinPage) {
		MaximumRowsCountinPage = maximumRowsCountinPage;
	}

	public int getRowsCounter() {
		return rowsCounter;
	}

	public void setRowsCounter(int rowsCounter) {
		this.rowsCounter = rowsCounter;
	}

	public ArrayList<Hashtable<String, String>> getRecords() {
		return records;
	}

	public void setRecords(ArrayList<Hashtable<String, String>> records) {
		this.records = records;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	public void addRecord(Hashtable<String, String> htblColNameValue) {
		records.add(htblColNameValue);
		// rowsCounter++;

	}

	public int getrecordPlace(Hashtable<String, String> x) {
		for (int c = 0; c < x.size(); c++) {
			if (records.get(c) == x)
				return c;

		}
		return -1;
	}

	public Hashtable<String, String> getRecord(String ColumnName,
			String ColumnValue) {
		Iterator i = records.iterator();
		Hashtable<String, String> x;
		while (i.hasNext()) {
			x = (Hashtable<String, String>) i.next();
			if (x.get(ColumnName).equals(ColumnValue)) {
				return x;
			}
		}
		return null;
	}

	public ArrayList<Hashtable<String, String>> getRecords(String ColumnName,
			String ColumnValue) {
		ArrayList res = new ArrayList();
		Iterator i = records.iterator();
		Hashtable<String, String> x;
		while (i.hasNext()) {
			x = (Hashtable<String, String>) i.next();
			if (x.get(ColumnName).equals(ColumnValue)) {
				res.add(x);
			}
		}
		return res;
	}

	public ArrayList<Hashtable<String, String>> getRecordLessthan(String TableName,
			String ColumnName, String ColumnValue) throws ClassNotFoundException, IOException {
		ArrayList res = new ArrayList();
		Iterator i = records.iterator();
		Hashtable<String, String> x;
		while (i.hasNext()) {
			x = (Hashtable<String, String>) i.next();
			Comparable O1=(Comparable) getValueIfValid( TableName,  ColumnName,  ColumnValue);
			Comparable O2=(Comparable) getValueIfValid( TableName,  ColumnName,  x.get(ColumnName));
			if (O2.compareTo(O1) < 0) {
				res.add(x);
			}
		}
		return res;
	}

	public ArrayList<Hashtable<String, String>> getRecordbiggerthan(String TableName,
			String ColumnName, String ColumnValue) throws ClassNotFoundException, IOException {
		ArrayList res = new ArrayList();
		Iterator i = records.iterator();
		Hashtable<String, String> x;
		while (i.hasNext()) {
			x = (Hashtable<String, String>) i.next();
			Comparable O1=(Comparable) getValueIfValid( TableName,  ColumnName,  ColumnValue);
			Comparable O2=(Comparable) getValueIfValid( TableName,  ColumnName,  x.get(ColumnName));
			if (O2.compareTo(O1) > 0) {
				res.add(x);
			}
		}
		return res;
	}
	public static Object getValueIfValid(String tableName, String columnName, String value) throws IOException, ClassNotFoundException{
		Hashtable<String, String> original = new Hashtable<String, String>();
		String currentLine = "";
		FileReader fileReader = new FileReader("data/metadata.csv");
		BufferedReader br = new BufferedReader(fileReader);
		while ((currentLine = br.readLine()) != null) {
			String[] result = currentLine.split(", ");
			if (result[0].equals(tableName)) {
				original.put(result[1], result[2]);
				//System.out.println(result[1] + ": " + result[2]);
			}
		}
		
		if(!original.containsKey(columnName)){
			return null;
		}
		
		String strColType = original.get(columnName);
		System.out.println("type: "+strColType);
		String strColValue = value;
		System.out.println("value: "+strColValue);
		Class x = Class.forName(strColType);
		// System.out.println(x);
		// Constructor conh structor = x.;

		Object y = null;
		try {
			y = x.getDeclaredConstructor(String.class).newInstance(
					strColValue);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			System.out.println("Invalid input");
			return null;

		}
		System.out.println("returned value: " + y.toString());
		return y;
	}
	public boolean removeRecord(String ColumnName, String ColumnValue) {

		Iterator i = ((ArrayList<Hashtable<String, String>>) records.clone())
				.iterator();
		Hashtable<String, String> x;
		while (i.hasNext()) {
			x = (Hashtable<String, String>) i.next();
			if (x.get(ColumnName).equals(ColumnValue)) {
				return records.remove(x);
			}
		}
		return false;
	}
	
}

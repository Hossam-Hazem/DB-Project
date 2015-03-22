import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

public class Page implements Serializable {
	private int MaximumRowsCountinPage;
	private int rowsCounter;
	private ArrayList<Hashtable<String, String>> records;
	private String pageName;
	private String pageDir;

	public Page(String pageDir, String pageName) throws IOException {
		this.MaximumRowsCountinPage = getPageSize();
		records = new ArrayList<Hashtable<String, String>>(
				MaximumRowsCountinPage);
		rowsCounter = 0;
		this.pageName = pageName;
		this.pageDir = pageDir;
		//System.out.println(this.MaximumRowsCountinPage);

		// create page
		writePage();
	}

	public void writePage() throws IOException {
		String path = pageDir + ".class";
		FileOutputStream fs = new FileOutputStream(path);
		ObjectOutputStream os = new ObjectOutputStream(fs);
		os.writeObject(this);
		os.close();
		fs.close();
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

	public void addRecord(Hashtable<String, String> htblColNameValue)
			throws IOException {
		records.add(htblColNameValue);
		rowsCounter++;
		writePage();
	}

	public void deleteRecord(Hashtable<String, String> htblColNameValue)
			throws IOException {

		boolean exists = false;
		boolean removeThis = false;
		for (int i = 0; i < records.size(); i++) {
			Hashtable<String, String> currentHtblColNameValue = records.get(i);
			Enumeration ColNames = htblColNameValue.keys();
			Enumeration currentColNames = currentHtblColNameValue.keys();
			while (ColNames.hasMoreElements()) {
				String ColName = (String) ColNames.nextElement();
				while (currentColNames.hasMoreElements()) {
					String currentColName = (String) currentColNames
							.nextElement();
					if (currentHtblColNameValue.get(currentColName) == htblColNameValue
							.get(ColName)) {
						exists = true;
						break;
					}
				}
				if (!exists) {
					removeThis = false;
					break;
				} else {
					removeThis = true;
				}
				exists = false;
			}
			if (removeThis) {
				Hashtable<String, String> tmp = records.get(i);
				tmp.clear();
				tmp = null;
			}
		}

		writePage();
	}

	public String getPageDir() {
		return pageDir;
	}

	public void setPageDir(String pageDir) {
		this.pageDir = pageDir;
	}
}
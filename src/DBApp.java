import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DBApp {
	static String tempTabe;

	@SuppressWarnings("rawtypes")
	public static void createTable(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws DBAppException, IOException {
		if (alreadyExist(strTableName) == false) {
			FileReader fileReader = new FileReader("data/metadata.csv");
			BufferedReader br = new BufferedReader(fileReader);
			String x = br.readLine();

			FileWriter fileWriter = new FileWriter("data/metadata.csv", true);

			if (x == null) {
				fileWriter
						.append("Table Name, Column Name, Column Type, Key, Indexed, References");
				fileWriter.append("\n");
			}

			Set set = htblColNameType.entrySet();
			Iterator it = set.iterator();
			while (it.hasNext()) {
				Map.Entry entry = (Map.Entry) it.next();
				String temp = strTableName
						+ ", "
						+ (String) entry.getKey()
						+ ", "
						+ (String) entry.getValue()
						+ ", "
						+ (entry.getKey().equals(strKeyColName) ? "true"
								: "false") + ", " + "false" + ", "
						+ htblColNameRefs.get(entry.getKey()) + "\n";

				fileWriter.append(temp);
				System.out.println(entry.getKey() + " : " + entry.getValue());
			}
			br.close();
			fileWriter.close();
			new Table(strTableName);
		}
		else{
			System.out.println("Table already Exists");
		}
		
		
	}

	

	public static boolean alreadyExist(String strTableName) throws IOException {
		boolean found = false;
		String currentLine = "";
		FileReader fileReader = new FileReader("data/metadata.csv");
		BufferedReader br = new BufferedReader(fileReader);
		while ((currentLine = br.readLine()) != null) {
			String[] result = currentLine.split(", ");
			if (result[0].equals(strTableName)) {
				found = true;
				break;
			}
		}
		br.close();
		return found;

	}

	public static void createIndex(String strTableName, String strColName)
			throws DBAppException, IOException {
		String currentLine = "";
		FileReader fileReader = new FileReader("data/metadata.csv");
		BufferedReader br = new BufferedReader(fileReader);
		ArrayList<String[]> x = new ArrayList<String[]>();

		while ((currentLine = br.readLine()) != null) {
			String[] temp = currentLine.split(", ");
			x.add(temp);
		}
		FileWriter fileWriter = new FileWriter("data/metadata.csv");
		for (int i = 0; i < x.size(); i++) {
			if (x.get(i)[0].equals(strTableName)
					&& x.get(i)[1].equals(strColName)) {
				fileWriter.append(x.get(i)[0] + ", " + x.get(i)[1] + ", "
						+ x.get(i)[2] + ", " + x.get(i)[3] + ", " + "true"
						+ ", " + x.get(i)[5] + "\n");
			} else {
				fileWriter.append(x.get(i)[0] + ", " + x.get(i)[1] + ", "
						+ x.get(i)[2] + ", " + x.get(i)[3] + ", " + x.get(i)[4]
						+ ", " + x.get(i)[5] + "\n");
			}
		}
		System.out.println("Index Done");
		br.close();
		fileWriter.close();
	}

	// Under Construction
	public static void insertIntoTable(String strTableName,
			Hashtable<String, String> htblColNameValue) throws DBAppException,
			ClassNotFoundException, IOException {
		String path = "data/tables/" + strTableName + "/" + strTableName
				+ ".bin";
		Table x = (Table) deserialize(path);
		if (x.getAllPages().size() == 0) {
			x.createPage();
			System.out.println("First page intialized");
		}

		path = x.getLastPagePath();

		Page lastPageinTable = (Page) deserialize(path);
		if (!lastPageinTable.isFull()) {
			lastPageinTable.addRecord(htblColNameValue);
			lastPageinTable
					.setRowsCounter(lastPageinTable.getRowsCounter() + 1);
		} else {
			x.createPage(); // already added in the method to the array
			System.out.println("New page created");
			path = x.getLastPagePath();
			lastPageinTable = (Page) deserialize(path);
			lastPageinTable.addRecord(htblColNameValue);
			lastPageinTable
					.setRowsCounter(lastPageinTable.getRowsCounter() + 1);

		}
		path = "data/tables/" + strTableName + "/" + strTableName + ".bin";
		serialize(path, x);
		path = x.getLastPagePath();
		serialize(path, lastPageinTable);

	}

	public void deleteFromTable(String strTableName,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException {

	}

	public static Object deserialize(String path) throws IOException,
			ClassNotFoundException {
		FileInputStream fi = new FileInputStream(path);
		ObjectInputStream os = new ObjectInputStream(fi);
		Object x = os.readObject();
		// System.out.println(((Page)x).getRecords().get(0));
		os.close();
		fi.close();
		return x;
	}

	public static void serialize(String path, Object x) throws IOException {
		FileOutputStream fs = new FileOutputStream(path);
		ObjectOutputStream os = new ObjectOutputStream(fs);
		os.writeObject(x);
		os.close();
		fs.close();
	}

	public static void main(String[] args) throws IOException, DBAppException,
			ClassNotFoundException {
		/*
		 * Hashtable<String, String> htblColNameType = new Hashtable<String,
		 * String>(); htblColNameType.put("col1", "str");
		 * htblColNameType.put("col2", "int"); htblColNameType.put("col3",
		 * "int"); htblColNameType.put("col4", "str");
		 * 
		 * Hashtable<String, String> htblColNameRefs = new Hashtable<String,
		 * String>(); htblColNameRefs.put("col1", "table1.id");
		 * 
		 * createTable("testAll6", htblColNameType, htblColNameRefs, "col2");
		 */
		// createIndex("testAll", "col3");

		// Clean csv file
		/*
		 * FileWriter fileWriter = new FileWriter("data/metadata.csv");
		 * fileWriter.append("");
		 */
		// System.out.println(alreadyExist("test10"));

		// makeTable("test4")
		/*
		 * FileInputStream fi = new FileInputStream("data/tables/test4.bin");
		 * ObjectInputStream os = new ObjectInputStream(fi); Table x =
		 * (Table)os.readObject(); System.out.println(x.getTableName());
		 * os.close(); fi.close();
		 */
		/*
		  for (int i = 0; i < 200; i++) { Hashtable<String, String> insertion =
		  new Hashtable<String, String>(); insertion.put("col1", "str");
		  insertion.put("col2", "int"); insertion.put("col3", "int");
		  insertion.put("col4", "str");
		  
		  insertIntoTable("testAll6", insertion); }
		 */
/*
		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("col1", "str");
		insertion.put("col2", "int");
		insertion.put("col3", "int");
		insertion.put("col4", "test");

		insertIntoTable("testAll6", insertion);
*/
		 //Page x = (Page)deserialize("data/tables/testAll6/pages/0.class");
		 //System.out.println(x.getRecords());

		// --------------------------------------------------------------------

	}
}

// createTable Done
// createInsex Done
// insertIntoTable Done --> lssa 7war el key bs

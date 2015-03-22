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

import BPTree.BTree;

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
		}
		//makeTable(strTableName);
		new Table(strTableName,strKeyColName);
	}
/*
	private static void makeTable(String strTableName) throws IOException {
		Table x = new Table(strTableName);
		String path = "data/tables/" + strTableName + "/" + strTableName
				+ ".bin";
		// make folder containing all table info
		File saveDir = new File("data/tables");
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		// make pages directory inside table folder
		saveDir = new File("data/tables/" + strTableName + "/" + "pages");
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		// make hashtable directory inside table folder
		saveDir = new File("data/tables/" + strTableName + "/" + "hashtable");
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		// make BTree directory inside table folder
		saveDir = new File("data/tables/" + strTableName + "/" + "BTree");
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		//
		 // FileOutputStream fs = new FileOutputStream(path); ObjectOutputStream
		 // os = new ObjectOutputStream(fs); os.writeObject(x); os.close();
		 // fs.close();
		 //
		serialize(path, x);
	}
*/

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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void createIndex(String strTableName, String strColName)
			throws DBAppException, IOException, ClassNotFoundException {
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
		br.close();
		fileWriter.close();
		////Indexing
		String Tablepath = "data/tables/" + strTableName + "/" + strTableName
				+ ".bin";
		Table T = (Table) deserialize(Tablepath);
		BTree B = new BTree();
		LinearHashtable L = new LinearHashtable();
		for(int i=0;i<T.getNameCounter();i++){
			String Pagepath = "data/tables/" + strTableName + "/" + "pages/"+i;
			Page P = (Page) deserialize(Pagepath);
			ArrayList<Hashtable<String, String>> AllRecords =P.getRecords();
			for(int j=0;j<P.getRowsCounter();j++){
				Hashtable<String, String> r =AllRecords.get(j); 
				String c =r.get(strColName);
				B.put(c, r);
				L.put(c, r);
			}
			
		}
		System.out.println("Index Done");
		
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
			System.out.println("YAAAAY First page intialized");
		}

		String lastPage = x.getAllPages().get(x.getAllPages().size() - 1);
		System.out.println(lastPage);
		System.out.println(lastPage);
		path = "data/tables/" + strTableName + "/" + "pages/" + lastPage
				+ ".class";
		Page lastPageinTable = (Page) deserialize(path);
		if (!lastPageinTable.isFull()) {
			lastPageinTable.addRecord(htblColNameValue);
			lastPageinTable
					.setRowsCounter(lastPageinTable.getRowsCounter() + 1);
		} else {
			x.createPage(); // already added in the method to the array
			lastPage = x.getAllPages().get(x.getAllPages().size() - 1);
			System.out.println("New page created");
			path = "data/tables/" + strTableName + "/" + "pages/" + lastPage
					+ ".class";
			lastPageinTable = (Page) deserialize(path);
			lastPageinTable.addRecord(htblColNameValue);
			lastPageinTable
					.setRowsCounter(lastPageinTable.getRowsCounter() + 1);

		}
		path = "data/tables/" + strTableName + "/" + strTableName + ".bin";
		serialize(path, x);
		path = "data/tables/" + strTableName + "/" + "pages/" + lastPage
				+ ".class";
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
		 * for (int i = 0; i < 200; i++) { Hashtable<String, String> insertion =
		 * new Hashtable<String, String>(); insertion.put("col1", "str");
		 * insertion.put("col2", "int"); insertion.put("col3", "int");
		 * insertion.put("col4", "str");
		 * 
		 * insertIntoTable("testAll2", insertion); }
		 */

		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("col1", "str");
		insertion.put("col2", "int");
		insertion.put("col3", "int");
		insertion.put("col4", "str");

		insertIntoTable("testAll6", insertion);

		// Page x = (Page)deserialize("data/testAll/0.class");
		// System.out.println(x.getRecords());

		// --------------------------------------------------------------------

	}
}

// createTable Done
// createInsex Done
// insertIntoTable Done --> lssa 7war el key bs

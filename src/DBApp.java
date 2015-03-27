import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import BPTree.BTree;

public class DBApp {
	static String tempTabe;
	static Hashtable<String, Object> virtualDirectory;


	public void init() {
		virtualDirectory = new Hashtable<String, Object>();
	}

	public static void saveAll() throws DBEngineException, IOException {

		Enumeration ColNames = virtualDirectory.keys();
		while (ColNames.hasMoreElements()) {
			String ColName = (String) ColNames.nextElement();
			serialize(ColName, virtualDirectory.get(ColName));
		}
		virtualDirectory.clear();

	}

	private static Object loadFileDyn(String Path)
			throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
		Object ret = virtualDirectory.get(Path);
		if (ret != null) {
			return ret;
		} else {
			return deserialize(Path);
		}
	}

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
								: "false") + ", " + (entry.getKey().equals(strKeyColName) ? "true"
										: "false") + ", "
						+ htblColNameRefs.get(entry.getKey()) + "\n";

				fileWriter.append(temp);
				System.out.println(entry.getKey() + " : " + entry.getValue());
			}
			br.close();
			fileWriter.close();
		}
		// makeTable(strTableName);
		new Table(strTableName, strKeyColName);
	}

	/*
	 * private static void makeTable(String strTableName) throws IOException {
	 * Table x = new Table(strTableName); String path = "data/tables/" +
	 * strTableName + "/" + strTableName + ".bin"; // make folder containing all
	 * table info File saveDir = new File("data/tables"); if (!saveDir.exists())
	 * { saveDir.mkdirs(); } // make pages directory inside table folder saveDir
	 * = new File("data/tables/" + strTableName + "/" + "pages"); if
	 * (!saveDir.exists()) { saveDir.mkdirs(); } // make hashtable directory
	 * inside table folder saveDir = new File("data/tables/" + strTableName +
	 * "/" + "hashtable"); if (!saveDir.exists()) { saveDir.mkdirs(); } // make
	 * BTree directory inside table folder saveDir = new File("data/tables/" +
	 * strTableName + "/" + "BTree"); if (!saveDir.exists()) { saveDir.mkdirs();
	 * } // // FileOutputStream fs = new FileOutputStream(path);
	 * ObjectOutputStream // os = new ObjectOutputStream(fs); os.writeObject(x);
	 * os.close(); // fs.close(); // serialize(path, x); }
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
		// //Indexing
		String Tablepath = "data/tables/" + strTableName + "/" + strTableName
				+ ".bin";
		// Table T = (Table) deserialize(Tablepath);
		Table T = (Table) loadFileDyn(Tablepath);
		T.addIndextoArray(strColName);
		BTree B = new BTree();
		LinearHashtable L = new LinearHashtable();
		for (int i = 0; i < T.getNameCounter(); i++) {
			String Pagepath = "data/tables/" + strTableName + "/" + "pages/"
					+ i+".class";
			// Page P = (Page) deserialize(Pagepath);
			Page P = (Page) loadFileDyn(Pagepath);

			ArrayList<Hashtable<String, String>> AllRecords = P.getRecords();
			for (int j = 0; j < P.getRowsCounter(); j++) {
				Hashtable<String, String> r = AllRecords.get(j);
				String c = r.get(strColName);
				B.put((Comparable) getValueIfValid(strTableName, strColName, c), Pagepath);
				L.put(c, Pagepath);
			}

		}
		String BTreePath = "data/tables/" + strTableName + "/" + "BTree/"
				+ strColName + ".bin";
		String LHTPath = "data/tables/" + strTableName + "/" + "hashtable/"
				+ strColName + ".bin";
		// serialize(Tablepath, T);
		virtualDirectory.put(Tablepath, T);
		// serialize(BTreePath, B);
		virtualDirectory.put(BTreePath, B);
		// serialize(LHTPath, L);
		virtualDirectory.put(LHTPath, L);
		System.out.println("Index Done");

	}

	// Under Construction
	public static void insertIntoTable(String strTableName,
			Hashtable<String, String> htblColNameValue) throws DBAppException,
			ClassNotFoundException, IOException {
		if (isValidInput(strTableName, htblColNameValue)) {
			String currentPagepath;
			String path = "data/tables/" + strTableName + "/" + strTableName
					+ ".bin";
			// Table x = (Table) deserialize(path);
			Table x = (Table) loadFileDyn(path);
			if (x.getAllPages().size() == 0) {
				x.createPage();
				System.out.println("YAAAAY First page intialized");
			}

			String lastPage = x.getAllPages().get(x.getAllPages().size() - 1);
			// System.out.println(lastPage);
			// System.out.println(lastPage);
			path = "data/tables/" + strTableName + "/" + "pages/" + lastPage
					+ ".class";
			// Page lastPageinTable = (Page) deserialize(path);
			Page lastPageinTable = (Page) loadFileDyn(path);
			if (!lastPageinTable.isFull()) {
				lastPageinTable.addRecord(htblColNameValue);
				lastPageinTable
						.setRowsCounter(lastPageinTable.getRowsCounter() + 1);
			} else {
				x.createPage(); // already added in the method to the array
				lastPage = x.getAllPages().get(x.getAllPages().size() - 1);
				System.out.println("New page created HOHOHOHO");
				path = "data/tables/" + strTableName + "/" + "pages/"
						+ lastPage + ".class";
				// lastPageinTable = (Page) deserialize(path);
				lastPageinTable = (Page) loadFileDyn(path);
				lastPageinTable.addRecord(htblColNameValue);
				lastPageinTable
						.setRowsCounter(lastPageinTable.getRowsCounter() + 1);

			}
			currentPagepath = path;
			path = "data/tables/" + strTableName + "/" + strTableName + ".bin";
			// serialize(path, x);
			virtualDirectory.put(path, x);
			path = "data/tables/" + strTableName + "/" + "pages/" + lastPage
					+ ".class";
			// serialize(path, lastPageinTable);
			virtualDirectory.put(path, lastPageinTable);
			// indexing
			ArrayList<String> Indexes = x.getIndexes();
			for (int c = 0; c < Indexes.size(); c++) {
				String index = Indexes.get(c);
				String BTreePath = "data/tables/" + strTableName + "/"
						+ "BTree/" + index + ".bin";
				String LHTPath = "data/tables/" + strTableName + "/"
						+ "hashtable/" + index + ".bin";
				// BTree B = (BTree) deserialize(BTreePath);
				BTree B = (BTree) loadFileDyn(BTreePath);
				// LinearHashtable L = (LinearHashtable) deserialize(LHTPath);
				LinearHashtable L = (LinearHashtable) loadFileDyn(LHTPath);
				String value = htblColNameValue.get(index);
				Comparable O = (Comparable) getValueIfValid(strTableName, index, value);
				B.put(O , currentPagepath);
				L.put(value, currentPagepath);
				// serialize(LHTPath, L);
				virtualDirectory.put(LHTPath, L);
				// serialize(BTreePath, B);
				virtualDirectory.put(BTreePath, B);
			}
		}

	}

	public static void deleteFromTable(String strTableName,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException, ClassNotFoundException, IOException {
		Iterator I = selectValueFromTableV2(strTableName, htblColNameValue,
				strOperator);
		String Tpath = "data/tables/" + strTableName + "/" + strTableName
				+ ".bin";
		// Table T = (Table) deserialize(Tpath);
		Table T = (Table) loadFileDyn(Tpath);
		String PrimaryKeyColumn = T.getIndexes().get(0);
		String BTreePath = "data/tables/" + strTableName + "/" + "BTree/"
				+ PrimaryKeyColumn + ".bin";
		String LHTPath = "data/tables/" + strTableName + "/" + "hashtable/"
				+ PrimaryKeyColumn + ".bin";
		// BTree B = (BTree) deserialize(BTreePath);
		BTree B = (BTree) loadFileDyn(BTreePath);
		// LinearHashtable L = (LinearHashtable) deserialize(LHTPath);
		LinearHashtable L = (LinearHashtable) loadFileDyn(LHTPath);

		ArrayList<String> indices = T.getIndexes();
		while (I.hasNext()) {
			
			Hashtable<String, String> r = (Hashtable<String, String>) I.next();

			if (r != null) {
				String PrimaryKeyValue = r.get(PrimaryKeyColumn);
				String RPath = (String) L.get(PrimaryKeyValue);
				// Page p = (Page) deserialize(RPath);
				Page p = (Page) loadFileDyn(RPath);
				p.removeRecord(PrimaryKeyColumn, PrimaryKeyValue);
				virtualDirectory.put(RPath, p);
				
				Iterator indicesI = indices.iterator();
				while(indicesI.hasNext()){
					String Column=(String) indicesI.next();
				 BTreePath = "data/tables/" + strTableName + "/" + "BTree/"
							+ Column + ".bin";
				 LHTPath = "data/tables/" + strTableName + "/" + "hashtable/"
						+ Column + ".bin";
				  B = (BTree) loadFileDyn(BTreePath);
					// LinearHashtable L = (LinearHashtable) deserialize(LHTPath);
				  L = (LinearHashtable) loadFileDyn(LHTPath);
				L.delete(r.get(Column));
				Comparable O = (Comparable) getValueIfValid(strTableName,Column,
						r.get(Column));
				B.delete( O);
				// serialize(RPath, p);
				// serialize(LHTPath, L);
				virtualDirectory.put(LHTPath, L);
				// serialize(BTreePath, B);
				virtualDirectory.put(BTreePath, B);
				}
			}
			
		}

	}




	public static ArrayList SelectRangeOneCondition(String strTable,String ColumnName,String ColumnAllValue) throws ClassNotFoundException, IOException{
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		ArrayList result = new ArrayList();
		// Table T = (Table) deserialize(tablepath);
		Table T = (Table) loadFileDyn(tablepath);
		String ColumnValue;
		String Columnrange = getOperator(ColumnAllValue);
		if (Columnrange.length() == 1) {
			ColumnValue = ColumnAllValue.substring(1);
		} else {
			ColumnValue = ColumnAllValue.substring(2);
		}

		if (T.getIndexes().contains(ColumnName)) {
			String BTreePath = "data/tables/" + strTable + "/"
					+ "BTree/" + ColumnName + ".bin";
			// BTree B = (BTree) deserialize(BTreePath);
			BTree B = (BTree) loadFileDyn(BTreePath);
			ArrayList tempo = new ArrayList();
			ArrayList tempoe = new ArrayList();
			ArrayList pathes = new ArrayList();
			ArrayList pathesB = new ArrayList();

			if (Columnrange.charAt(0) == '>') {
				Comparable O=(Comparable) getValueIfValid( strTable,  ColumnName,  ColumnValue);
				pathes = B.getbiggerthan(O);
				Iterator pathesI = pathes.iterator();
				ArrayList PagesScanned = new ArrayList();
				while (pathesI.hasNext()) {
					String PagePath = (String) pathesI.next();
					if (!PagesScanned.contains(PagePath)) {
						// Page p = (Page) deserialize((PagePath));
						Page p = (Page) loadFileDyn((PagePath));
						Iterator Itemp = p.getRecordbiggerthan(strTable,
								ColumnName, ColumnValue).iterator();
						while (Itemp.hasNext()) {
							Hashtable<String, String> r = (Hashtable<String, String>) Itemp
									.next();
								result.add(r);
							
						}
						PagesScanned.add(PagePath);
					}

				}
			}

			if (Columnrange.charAt(0) == '<') {
				Comparable O=(Comparable) getValueIfValid( strTable,  ColumnName,  ColumnValue);
				pathes = B.getSmallerthan(O);
				Iterator pathesI = pathes.iterator();
				ArrayList PagesScanned = new ArrayList();
				while (pathesI.hasNext()) {
					String PagePath = (String) pathesI.next();
					if (!PagesScanned.contains(PagePath)) {
						// Page p = (Page) deserialize((PagePath));
						Page p = (Page) loadFileDyn((PagePath));
						Iterator Itemp = p.getRecordLessthan(strTable,
								ColumnName, ColumnValue).iterator();
						while (Itemp.hasNext()) {
							Hashtable<String, String> r = (Hashtable<String, String>) Itemp
									.next();

								result.add(r);

							
						}
						PagesScanned.add(PagePath);

					}

				}
			}
			Comparable O = (Comparable) getValueIfValid(strTable, ColumnName,ColumnValue);
			if (Columnrange.length() != 1
					&& B.search(O) != null) {

				String path = (String) B.search(O);
				// Page p = (Page) deserialize((path));
				Page p = (Page) loadFileDyn((path));
				Hashtable<String, String> r = (Hashtable<String, String>) p
						.getRecord(ColumnName, ColumnValue);
					result.add(r);
				

			}

		} else {
			Iterator PagesI = T.getAllPages().iterator();
			while (PagesI.hasNext()) {
				ArrayList tempo = new ArrayList();
				ArrayList tempoe = new ArrayList();
				String Pname = (String) PagesI.next();
				String PagePath = "data/tables/" + strTable + "/"
						+ "pages/" + Pname + ".class";
				// Page p = (Page) deserialize(PagePath);
				Page p = (Page) loadFileDyn(PagePath);
				if (Columnrange.length() != 1)
					tempoe = p.getRecords(ColumnName, ColumnValue);

				if (Columnrange.charAt(0) == '>')
					tempo = p.getRecordbiggerthan(strTable,ColumnName,
							ColumnValue);

				if (Columnrange.charAt(0) == '<')
					tempo = p
							.getRecordLessthan(strTable,ColumnName, ColumnValue);
				
				Iterator tempoI = tempo.iterator();
				while (tempoI.hasNext()) {
					Hashtable<String, String> r = (Hashtable<String, String>) tempoI
							.next();
						result.add(r);
				}
				Iterator tempoeI = tempoe.iterator();
				while (tempoeI.hasNext()) {
					Hashtable<String, String> r = (Hashtable<String, String>) tempoeI
							.next();
						result.add(r);
				}

			}

		}
		return result;
	}
	public static String getOperator(String x) {
		String res = "";
		if (x.charAt(1) == '=') {
			// System.out.println("" + x.charAt(0) + x.charAt(1));
			return "" + x.charAt(0) + x.charAt(1);
		}

		else
			return x.charAt(0) + "";
	}

	public static void ADDrecord_SelectHelper(String Path, String colName,
			String colVal) {

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

	public static ArrayList SelectValueQueryOneCondition(String strTable,
			String ColumnName, String ColumnValue)
			throws ClassNotFoundException, IOException {
		ArrayList result = new ArrayList();
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		// Table T = (Table) deserialize(tablepath);
		Table T = (Table) loadFileDyn(tablepath);
		if (T.getIndexes().contains(ColumnName)) {
			String LHTPath = "data/tables/" + strTable + "/" + "hashtable/"
					+ ColumnName + ".bin";
			// LinearHashtable L = (LinearHashtable)
			// deserialize(LHTPath);
			LinearHashtable L = (LinearHashtable) loadFileDyn(LHTPath);
			String RecordPath = (String) L.get(ColumnValue);
			if (RecordPath != null) {
				// Page p = (Page) deserialize(RecordPath);
				Page p = (Page) loadFileDyn(RecordPath);
				Hashtable<String, String> r = p.getRecord(ColumnName,
						ColumnValue);
				result.add(r);
			}

		} else {
			Iterator PagesI = T.getAllPages().iterator();
			while (PagesI.hasNext()) {
				String Pname = (String) PagesI.next();
				String PagePath = "data/tables/" + strTable + "/" + "pages/"
						+ Pname + ".class";
				// Page p = (Page) deserialize(PagePath);
				Page p = (Page) loadFileDyn(PagePath);
				Hashtable<String, String> r = p.getRecord(ColumnName,
						ColumnValue);
				ArrayList<Hashtable<String, String>> allRecordsInPage = p
						.getRecords();
				for (int i = 0; i < allRecordsInPage.size(); i++) {
					if (allRecordsInPage.get(i).get(ColumnName)
							.equals(ColumnValue)) {
						result.add(allRecordsInPage.get(i));
					}
				}
			}
		}
		return result;
	}

	public static Iterator selectRangeFromTableV2(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator) throws ClassNotFoundException, IOException{
		ArrayList result = new ArrayList();
		ArrayList tempresult = new ArrayList();
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		// Table T = (Table) deserialize(tablepath);
		Table T = (Table) loadFileDyn(tablepath);
		Iterator coloumnsI = htblColNameValue.keySet().iterator();
		if (strOperator.equals("OR")) {
			while (coloumnsI.hasNext()) {
				String ColumnName = (String) coloumnsI.next();
				System.out.println(ColumnName); // major
				String ColumnValue = htblColNameValue.get(ColumnName);
				tempresult=(SelectRangeOneCondition(strTable, ColumnName, ColumnValue));
				Iterator tempresultI=tempresult.iterator();
				while(tempresultI.hasNext()){
					Hashtable<String, String> record =(Hashtable<String, String>) tempresultI.next();
					if(!result.contains(record))
						result.add(record);
				}
			}
			for (int i = 0; i < result.size(); i++) {
				System.out.println(result.get(i));
			}
			return result.iterator();
		}
		if (strOperator.equals("AND")) {
			boolean flag = false;
			while (coloumnsI.hasNext()) {
				String ColumnName = (String) coloumnsI.next();
				String ColumnAllValue = htblColNameValue.get(ColumnName);
				System.out.println("Oo "+ColumnAllValue);
				if (flag == false) {
					tempresult=(SelectRangeOneCondition(strTable, ColumnName, ColumnAllValue));
					Iterator tempresultI=tempresult.iterator();
					while(tempresultI.hasNext()){
						Hashtable<String, String> record =(Hashtable<String, String>) tempresultI.next();
						if(!result.contains(record))
							result.add(record);
					}
					flag = true;
				} else { // low flag mesh be false i.e. mesh 2wl iteration
							// 3lshan yeloop 3ala result mesh database 3lshan
							// AND kan nefse 23mlha recursion <3

					ArrayList temp = new ArrayList();
					temp = (ArrayList) result.clone();
					Iterator ResultI = temp.iterator();
					while (ResultI.hasNext()) {
						Hashtable<String, String> Record = (Hashtable<String, String>) ResultI
								.next();
						String Columnrange = getOperator(ColumnAllValue);
						String ColumnValue;
						if (Columnrange.length() == 1) {
							ColumnValue = ColumnAllValue.substring(1);
						} else {
							ColumnValue = ColumnAllValue.substring(2);
						}
						Comparable O1 =  (Comparable) getValueIfValid(strTable, ColumnName,ColumnValue );
						Comparable O2 =  (Comparable) getValueIfValid(strTable, ColumnName,Record.get(ColumnName));
						if (Columnrange.length() == 2) {
							if (O2.compareTo(O1) != 0)
								if (Columnrange.charAt(0) == '>') {
									if (O2.compareTo(
											O1) < 0)
										result.remove(Record);
								}
							if (Columnrange.charAt(0) == '<') {
								if (O2.compareTo(
										O1) > 0)
									result.remove(Record);
							}
						} else {
							if (Columnrange.charAt(0) == '>') {
								if (O2.compareTo(
										O1) <= 0)
									result.remove(Record);
							}
							if (Columnrange.charAt(0) == '<') {
								if (O2.compareTo(
										O1) >= 0)
									result.remove(Record);
							}
						}

					}
				}

			}
			for (int i = 0; i < result.size(); i++) {
				System.out.println(result.get(i));
			}

			return result.iterator();
		}
		return null;
		
	}

	public static Iterator selectValueFromTableV2(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException, ClassNotFoundException, IOException {
		ArrayList result = new ArrayList();
		ArrayList tempresult = new ArrayList();
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		// Table T = (Table) deserialize(tablepath);
		Table T = (Table) loadFileDyn(tablepath);
		Iterator coloumnsI = htblColNameValue.keySet().iterator();
		if (strOperator.equals("OR")) {
			while (coloumnsI.hasNext()) {
				String ColumnName = (String) coloumnsI.next();
				System.out.println(ColumnName); // major
				String ColumnValue = htblColNameValue.get(ColumnName);
				tempresult = (SelectValueQueryOneCondition(strTable,
						ColumnName, ColumnValue));
				Iterator tempresultI = tempresult.iterator();
				while (tempresultI.hasNext()) {
					Hashtable<String, String> record = (Hashtable<String, String>) tempresultI
							.next();
					if (!result.contains(record))
						result.add(record);
				}
			}
			for (int i = 0; i < result.size(); i++) {
				System.out.println(result.get(i));
			}
			return result.iterator();
		}
		if (strOperator.equals("AND")) {
			boolean flag = false;
			while (coloumnsI.hasNext()) {
				String ColumnName = (String) coloumnsI.next();
				String ColumnValue = htblColNameValue.get(ColumnName);
				if (flag == false) {
					tempresult = (SelectValueQueryOneCondition(strTable,
							ColumnName, ColumnValue));
					Iterator tempresultI = tempresult.iterator();
					while (tempresultI.hasNext()) {
						Hashtable<String, String> record = (Hashtable<String, String>) tempresultI
								.next();
						if (!result.contains(record))
							result.add(record);
					}
					flag = true;
				} else { // low flag mesh be false i.e. mesh 2wl iteration
							// 3lshan yeloop 3ala result mesh database 3lshan
							// AND kan nefse 23mlha recursion <3

					ArrayList temp = new ArrayList();
					temp = (ArrayList) result.clone();
					Iterator ResultI = temp.iterator();
					while (ResultI.hasNext()) {
						Hashtable<String, String> Record = (Hashtable<String, String>) ResultI
								.next();
						if (!Record.get(ColumnName).equals(ColumnValue))
							result.remove(Record);

					}
				}

			}
			for (int i = 0; i < result.size(); i++) {
				System.out.println(result.get(i));
			}

			return result.iterator();
		}
		return null;

	}
	
	public static boolean isValidInput(String strTableName,
			Hashtable<String, String> htblColNameValue) throws IOException,
			ClassNotFoundException {

		// contains column names and column types of input table
		Hashtable<String, String> original = new Hashtable<String, String>();
		String currentLine = "";
		FileReader fileReader = new FileReader("data/metadata.csv");
		BufferedReader br = new BufferedReader(fileReader);
		while ((currentLine = br.readLine()) != null) {
			String[] result = currentLine.split(", ");
			if (result[0].equals(strTableName)) {
				original.put(result[1], result[2]);
				//System.out.println(result[1] + ": " + result[2]);
			}
		}
		Set set = htblColNameValue.entrySet();
		Iterator it = set.iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();

			System.out.println(entry.getKey() + " : " + entry.getValue());
			if (!original.containsKey(entry.getKey())) {

				System.out.println("Column does not exist in that table");
				return false;
			}

			String strColType = original.get(entry.getKey());
			//System.out.println(strColType);
			String strColValue = (String) entry.getValue();
			//System.out.println(strColValue);
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
				return false;

			}
			//System.out.println(y);

		}

		// TO BE REMOVED
		return true;

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
	
	public static void main(String[] args) throws ClassNotFoundException, DBAppException, IOException, DBEngineException   {

		// test save all by doing the following : go to ===>
		/*
		 * init();
		 * 
		 * Hashtable<String, String> htblColNameType = new Hashtable<String,
		 * String>(); htblColNameType.put("col1", "str");
		 * htblColNameType.put("col2", "int"); htblColNameType.put("col3",
		 * "int"); htblColNameType.put("col4", "str");
		 * 
		 * Hashtable<String, String> htblColNameRefs = new Hashtable<String,
		 * String>(); htblColNameRefs.put("col1", "table1.id");
		 * 
		 * // ===> execute once and comment createTable and execute multiple
		 * times createTable("testAll", htblColNameType, htblColNameRefs,
		 * "col2");
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
		 * for (int i = 0; i < 300; i++) { Hashtable<String, String> insertion =
		 * new Hashtable<String, String>(); insertion.put("col1", "str");
		 * insertion.put("col2", "int"); insertion.put("col3", "int");
		 * insertion.put("col4", "str");
		 * 
		 * insertIntoTable("testAll", insertion); }
		 * 
		 * saveAll();
		 */
		/*
		 * Hashtable<String, String> insertion = new Hashtable<String,
		 * String>(); insertion.put("col1", "str"); insertion.put("col2",
		 * "int"); insertion.put("col3", "int"); insertion.put("col4", "str");
		 * 
		 * insertIntoTable("testAll6", insertion);
		 */
		/*
		 * Hashtable<String, String> htblColNameType = new Hashtable<String,
		 * String>(); htblColNameType.put("name", "str");
		 * htblColNameType.put("age", "int"); htblColNameType.put("ID","int");
		 * htblColNameType.put("major", "str");
		 * 
		 * Hashtable<String, String> htblColNameRefs = new Hashtable<String,
		 * String>();
		 * 
		 * createTable("testSelectFromTable3", htblColNameType, htblColNameRefs,
		 * "ID");
		 * 
		 * Hashtable<String, String> insertion = new Hashtable<String,
		 * String>(); insertion.put("name", "omar"); insertion.put("age", "2");
		 * insertion.put("ID", "10999"); insertion.put("major", "cs");
		 * 
		 * insertIntoTable("testSelectFromTable3", insertion);
		 * 
		 * 
		 * insertion = new Hashtable<String, String>(); insertion.put("name",
		 * "hossam"); insertion.put("age", "3"); insertion.put("ID", "286205");
		 * insertion.put("major", "cs");
		 * 
		 * insertIntoTable("testSelectFromTable3", insertion);
		 * 
		 * 
		 * Page p =
		 * (Page)deserialize("data/tables/testSelectFromTable3/Pages/0.class");
		 * System.out.println("All Records: " + p.getRecords());
		 * 
		 * BTree x =
		 * (BTree)deserialize("data/tables/testSelectFromTable3/BTree/ID.bin");
		 * x.print();
		 * 
		 * Hashtable<String, String> htblColNameValue = new Hashtable<String,
		 * String>(); htblColNameValue.put("ID", "10999");
		 * htblColNameValue.put("name", "hossam");
		 * 
		 * 
		 * Iterator i =selectValueFromTable("testSelectFromTable3",
		 * htblColNameValue, "OR"); int c =0; while(i.hasNext()){ i.next(); c++;
		 * } System.out.println(c);
		 */
		// Page x = (Page)deserialize("data/testAll/0.class");
		// System.out.println(x.getRecords());

		// --------------------------------------------------------------------
		// delete test
		/*
		 * Hashtable<String, String> htblColNameType = new Hashtable<String,
		 * String>(); htblColNameType.put("name", "str");
		 * htblColNameType.put("age", "int"); htblColNameType.put("ID", "int");
		 * htblColNameType.put("major", "str");
		 * 
		 * Hashtable<String, String> htblColNameRefs = new Hashtable<String,
		 * String>();
		 * 
		 * createTable("testDelete", htblColNameType, htblColNameRefs, "ID");
		 * 
		 * Hashtable<String, String> insertion = new Hashtable<String,
		 * String>(); insertion.put("name", "omar"); insertion.put("age", "2");
		 * insertion.put("ID", "10999"); insertion.put("major", "cs");
		 * 
		 * insertIntoTable("testDelete", insertion);
		 * 
		 * insertion = new Hashtable<String, String>(); insertion.put("name",
		 * "hossam"); insertion.put("age", "3"); insertion.put("ID", "286205");
		 * insertion.put("major", "cs");
		 * 
		 * insertIntoTable("testDelete", insertion);
		 * 
		 * Page p = (Page) deserialize("data/tables/testDelete/Pages/0.class");
		 * System.out.println("All Records: " + p.getRecords());
		 * 
		 * BTree x = (BTree) deserialize("data/tables/testDelete/BTree/ID.bin");
		 * x.print();
		 * 
		 * Hashtable<String, String> htblColNameValue = new Hashtable<String,
		 * String>(); htblColNameValue.put("ID", "286205"); //
		 * htblColNameValue.put("name", "hossam"); deleteFromTable("testDelete",
		 * htblColNameValue, "OR");
		 * 
		 * p = (Page) deserialize("data/tables/testDelete/Pages/0.class");
		 * System.out.println("All Records: " + p.getRecords());
		 * 
		 * x = (BTree) deserialize("data/tables/testDelete/BTree/ID.bin");
		 * x.print();
		 */
		// test range test
		/*
		 * Hashtable<String, String> htblColNameType = new Hashtable<String,
		 * String>(); htblColNameType.put("name", "str");
		 * htblColNameType.put("age", "int"); htblColNameType.put("ID", "int");
		 * htblColNameType.put("major", "str");
		 * 
		 * Hashtable<String, String> htblColNameRefs = new Hashtable<String,
		 * String>();
		 * 
		 * createTable("testrangeor", htblColNameType, htblColNameRefs, "ID");
		 * 
		 * Hashtable<String, String> insertion = new Hashtable<String,
		 * String>(); insertion.put("name", "omar"); insertion.put("age", "2");
		 * insertion.put("ID", "10999"); insertion.put("major", "cs");
		 * 
		 * insertIntoTable("testrangeor", insertion);
		 * 
		 * insertion = new Hashtable<String, String>(); insertion.put("name",
		 * "hossam"); insertion.put("age", "3"); insertion.put("ID", "286205");
		 * insertion.put("major", "cs");
		 * 
		 * insertIntoTable("testrangeor", insertion);
		 * 
		 * insertion = new Hashtable<String, String>(); insertion.put("name",
		 * "kareem"); insertion.put("age", "5"); insertion.put("ID", "2810989");
		 * insertion.put("major", "DMET");
		 * 
		 * insertIntoTable("testrangeor", insertion);
		 */

		// Page p = (Page) deserialize("data/tables/testrangeor/Pages/0.class");
		// System.out.println("All Records: " + p.getRecords());

		// BTree x = (BTree)
		// deserialize("data/tables/testrangeor/BTree/age.bin");
		// x.print();

		/*
		 * createIndex("testrangeor1", "age"); Hashtable<String, String>
		 * htblColNameRange = new Hashtable<String, String>();
		 * htblColNameRange.put("age", "<=3"); // htblColNameRange.put("age",
		 * "<=6"); // htblColNameValue.put("name", "hossam"); Iterator I =
		 * selectRangeFromTable("testrangeor1", htblColNameRange, "OR");
		 * 
		 * while (I.hasNext()) { System.out.println("done " +
		 * I.next().toString()); }
		 */


		// test V2

		/*
		 * init();
		 * 
		 * Hashtable<String, String> htblColNameType = new Hashtable<String,
		 * String>(); htblColNameType.put("name", "str");
		 * htblColNameType.put("age", "int"); htblColNameType.put("ID", "int");
		 * htblColNameType.put("major", "str");
		 * 
		 * Hashtable<String, String> htblColNameRefs = new Hashtable<String,
		 * String>();
		 * 
		 * createTable("testValueV2", htblColNameType, htblColNameRefs, "ID");
		 * 
		 * Hashtable<String, String> insertion = new Hashtable<String,
		 * String>(); insertion.put("name", "omar"); insertion.put("age", "2");
		 * insertion.put("ID", "2810999"); insertion.put("major", "cs");
		 * 
		 * insertIntoTable("testValueV2", insertion);
		 * 
		 * insertion = new Hashtable<String, String>(); insertion.put("name",
		 * "hossam"); insertion.put("age", "3"); insertion.put("ID", "286205");
		 * insertion.put("major", "cs");
		 * 
		 * insertIntoTable("testValueV2", insertion);
		 * 
		 * insertion = new Hashtable<String, String>(); insertion.put("name",
		 * "kareem"); insertion.put("age", "5"); insertion.put("ID", "2810989");
		 * insertion.put("major", "DMET");
		 * 
		 * insertIntoTable("testValueV2", insertion);
		 * 
		 * 
		 * 
		 * Hashtable<String, String> htblColNameRange = new Hashtable<String,
		 * String>(); htblColNameRange.put("age", "<286205"); //
		 * htblColNameRange.put("age","5"); // htblColNameRange.put("name",
		 * "omar"); Iterator I = selectRangeFromTable("testValueV2",
		 * htblColNameRange, "OR");
		 * 
		 * while (I.hasNext()) { System.out.println("done " +
		 * I.next().toString()); }
		 */
		/*
		String strColType = "java.lang.Character";
		 String strColValue = "omar";
		 Class x = Class.forName( strColType );
		 System.out.println(x);
		 //Constructor conh   structor = x.;
		 
		 Object y = null;
		try {
			y = x.getDeclaredConstructor(String.class).newInstance(strColValue);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			System.out.println("Invalid input, FOCUS :@");
			
		}
		 System.out.println(y);
*/
		/*
		init();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("name", "str");
		htblColNameType.put("age", "int");
		htblColNameType.put("ID", "int");
		htblColNameType.put("major", "str");

		Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();

		createTable("testIsValid", htblColNameType, htblColNameRefs, "ID");
		*/
		/*
		init();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("age", "java.lang.Integer");
		htblColNameType.put("ID", "java.lang.Integer");
		htblColNameType.put("major", "java.lang.String");

		Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();

		createTable("testIsValid2", htblColNameType, htblColNameRefs, "ID");
		*/
		/*
		Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
		//htblColNameValue.put("m", "2810999");
		htblColNameValue.put("ID", "2810999");
		htblColNameValue.put("name", "Omar");
		
		System.out.println(isValidInput("testIsValid2", htblColNameValue));
		*/
		//---------------------------------------------------------------------------------
		// test getValueIfValid
		/*
		init();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("age", "java.lang.Integer");
		htblColNameType.put("ID", "java.lang.Long");
		htblColNameType.put("major", "java.lang.String");

		Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();

		createTable("testvalueRange", htblColNameType, htblColNameRefs, "ID");
		
		  Hashtable<String, String> insertion = new Hashtable<String,
		  String>(); insertion.put("name", "omar"); insertion.put("age", "2");
		  insertion.put("ID", "2810999"); insertion.put("major", "cs");
		  
		  insertIntoTable("testvalueRange", insertion);
		  
		  insertion = new Hashtable<String, String>(); insertion.put("name",
		  "hossam"); insertion.put("age", "3"); insertion.put("ID", "286205");
		  insertion.put("major", "cs");
		  
		  insertIntoTable("testvalueRange", insertion);
		  
		  insertion = new Hashtable<String, String>(); insertion.put("name",
		  "kareem"); insertion.put("age", "5"); insertion.put("ID", "2810989");
		  insertion.put("major", "DMET");
		  
		  insertIntoTable("testvalueRange", insertion);
		  
		  
		  
		  Hashtable<String, String> htblColNameRange = new Hashtable<String,
		  String>();
		  
		  htblColNameRange.put("age", ">0"); //
		  //htblColNameRange.put("ID",">=286205"); 
		 // htblColNameRange.put("name", "omar");
		 // htblColNameRange.put("major", "cs");
		 // deleteFromTable("testDeleteV21", htblColNameRange, "OR");
		//  htblColNameRange.put("major", "cs");
		  Iterator I = selectRangeFromTableV2("testvalueRange",
		  htblColNameRange, "AND");
		  while (I.hasNext()) { System.out.println("BEFORE " +
		  I.next().toString()); }

		  */
		/*
		init();
		Page p = (Page) deserialize("data/tables/testCreateTable3/pages/0.class");
		System.out.println("All Records: " + p.getRecords());

		BTree x = (BTree) deserialize("data/tables/testCreateTable3/BTree/ID.bin");
		x.print();
		*/
		/*
		// NO Effect
		Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
		htblColNameValue.put("ID", "30303030");
		//htblColNameValue.put("name", "student2");
		deleteFromTable("testCreateTable3", htblColNameValue, "OR");

		p = (Page) deserialize("data/tables/testCreateTable3/pages/0.class");
		System.out.println("All Records: " + p.getRecords());

		x = (BTree) deserialize("data/tables/testCreateTable3/BTree/ID.bin");
		x.print();
		saveAll();
		*/
		/*
		init();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("age", "java.lang.Integer");
		htblColNameType.put("ID", "java.lang.Long");
		htblColNameType.put("major", "java.lang.String");

		Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();

		createTable("testvalueRange", htblColNameType, htblColNameRefs, "ID");
		
		  Hashtable<String, String> insertion = new Hashtable<String,
		  String>(); insertion.put("name", "omar"); insertion.put("age", "2");
		  insertion.put("ID", "2810999"); insertion.put("major", "cs");
		  
		  insertIntoTable("testvalueRange", insertion);
		  
		  insertion = new Hashtable<String, String>(); insertion.put("name",
		  "hossam"); insertion.put("age", "3"); insertion.put("ID", "286205");
		  insertion.put("major", "cs");
		  
		  insertIntoTable("testvalueRange", insertion);
		  
		  insertion = new Hashtable<String, String>(); insertion.put("name",
		  "kareem"); insertion.put("age", "5"); insertion.put("ID", "2810989");
		  insertion.put("major", "DMET");
		  
		  insertIntoTable("testvalueRange", insertion);
		  
		  
		  
		  Hashtable<String, String> htblColNameRange = new Hashtable<String,
		  String>();
		  
		  htblColNameRange.put("age", "<5"); //
		  htblColNameRange.put("ID",">=286205"); 
		 // htblColNameRange.put("name", "omar");
		 // htblColNameRange.put("major", "cs");
		 // deleteFromTable("testDeleteV21", htblColNameRange, "OR");
		//  htblColNameRange.put("major", "cs");
		  Iterator I = selectRangeFromTableV2("testvalueRange",
		  htblColNameRange, "AND");
		  
		  while (I.hasNext()) { System.out.println("done " +
		  I.next().toString()); }
		  */
		//init();
		Page p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());

		// Nothing will be returned... AND
		Hashtable<String, String> select = new Hashtable<String, String>();
		select.put("ID", ">3");
		Iterator I = selectRangeFromTableV2("testCreateTable", select,
				"OR");

		while (I.hasNext()) {
			System.out.println("done: " + I.next().toString());
		}

		
	}
}

// createTable Done
// createInsex Done
// insertIntoTable Done --> lssa 7war el key bs
// Edit already exists and call it in create table -----> Kareem 
//Edit is valid input and call it in create table -----> Kareem 
//Edit get value if valid and call it in create table -----> Kareem 
// Look at last point in pdf

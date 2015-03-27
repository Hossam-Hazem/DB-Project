package CodeVision;

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

import libs.BTree;
import libs.LinearHashtable;

public class DBApp {
	static String tempTabe;
	static Hashtable<String, Object> virtualDirectory;

	public static void init() {
		virtualDirectory = new Hashtable<String, Object>();
	}

	public static void saveAll() throws DBEngineException, IOException {

		Enumeration ColNames = virtualDirectory.keys();
		while (ColNames.hasMoreElements()) {
			String ColName = (String) ColNames.nextElement();
			if (ColName.equals("data\\metadata.csv")) {
				writeMeta((String) virtualDirectory.get(ColName));
			} else {
				serialize(ColName, virtualDirectory.get(ColName));
			}
		}
		virtualDirectory.clear();

	}

	private static Object loadFileDyn(String Path)
			throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
		Object ret = virtualDirectory.get(Path);
		if (Path.equals("data\\metadata.csv")) {
			if (ret != null) {
				return ret;
			} else {
				return readMetaFromDisk();
			}
		} else {
			if (ret != null) {
				return ret;
			} else {
				return deserialize(Path);
			}
		}

	}

	@SuppressWarnings("rawtypes")
	public static void createTable(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws DBAppException, IOException, ClassNotFoundException {
		if (alreadyExist(strTableName) == false) {
			updateMeta(htblColNameType, htblColNameRefs, strKeyColName,
					strTableName);
		}
		// makeTable(strTableName);
		new Table(strTableName, strKeyColName);
	}

	private static String readMetaFromDisk() throws IOException {
		// TODO Auto-generated method stub
		FileReader fileReader = new FileReader("data\\metadata.csv");
		BufferedReader br = new BufferedReader(fileReader);
		String tmp = "";
		String metaInfo = "";
		while ((tmp = br.readLine()) != null) {
			metaInfo += tmp + "\n";
		}
		br.close();
		return metaInfo;
	}

	private static void updateMeta(Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName,
			String strTableName) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub

		String meta = (String) loadFileDyn("data\\metadata.csv");

		String metaInfo = "";
		if (meta.equals("")) {
			metaInfo += "Table Name, Column Name, Column Type, Key, Indexed, References"
					+ "\n";
		}

		Set set = htblColNameType.entrySet();
		Iterator it = set.iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			String temp = strTableName + ", " + (String) entry.getKey() + ", "
					+ (String) entry.getValue() + ", "
					+ (entry.getKey().equals(strKeyColName) ? "true" : "false")
					+ ", "
					+ (entry.getKey().equals(strKeyColName) ? "true" : "false")
					+ ", " + htblColNameRefs.get(entry.getKey()) + "\n";

			metaInfo += temp;
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}

		virtualDirectory.put("data\\metadata.csv", metaInfo);
	}

	private static void writeMeta(String metaInfo) throws IOException {
		FileWriter fileWriter = new FileWriter("data\\metadata.csv", true);
		fileWriter.append(metaInfo);
		fileWriter.close();
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
		String[] br = ((String) readMetaFromDisk()).split("\n");
		for (String line : br) {
			String[] result = line.split(", ");
			if (result[0].equals(strTableName)) {
				found = true;
				break;
			}
		}
		return found;

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void createIndex(String strTableName, String strColName)
			throws DBAppException, IOException, ClassNotFoundException {

		ArrayList<String[]> x = new ArrayList<String[]>();

		String[] br = ((String) readMetaFromDisk()).split("\n");
		for (String line : br) {
			String[] temp = line.split(", ");
			x.add(temp);
		}

		String MetaInfo = "";
		for (int i = 0; i < x.size(); i++) {
			if (x.get(i)[0].equals(strTableName)
					&& x.get(i)[1].equals(strColName)) {
				MetaInfo += (x.get(i)[0] + ", " + x.get(i)[1] + ", "
						+ x.get(i)[2] + ", " + x.get(i)[3] + ", " + "true"
						+ ", " + x.get(i)[5] + "\n");
			} else {
				MetaInfo += (x.get(i)[0] + ", " + x.get(i)[1] + ", "
						+ x.get(i)[2] + ", " + x.get(i)[3] + ", " + x.get(i)[4]
						+ ", " + x.get(i)[5] + "\n");
			}
		}

		writeMeta(MetaInfo);

		// Indexing
		Table T = new Table(strTableName);
		T.addIndextoArray(strColName);
		BTree B = new BTree();
		LinearHashtable L = new LinearHashtable();
		for (int i = 0; i < T.getNameCounter(); i++) {
			String Pagepath = "data/tables/" + strTableName + "/" + "pages/"
					+ i + ".class";
			// Page P = (Page) deserialize(Pagepath);
			Page P = (Page) loadFileDyn(Pagepath);

			ArrayList<Hashtable<String, String>> AllRecords = P.getRecords();
			for (int j = 0; j < P.getRowsCounter(); j++) {
				Hashtable<String, String> r = AllRecords.get(j);
				String c = r.get(strColName);
				B.put((Comparable) getValueIfValid(strTableName, strColName, c),
						Pagepath);
				L.put(c, Pagepath);
			}

		}
		String BTreePath = "data/tables/" + strTableName + "/" + "BTree/"
				+ strColName + ".bin";
		String LHTPath = "data/tables/" + strTableName + "/" + "hashtable/"
				+ strColName + ".bin";
		// serialize(Tablepath, T);
		// virtualDirectory.put(Tablepath, T);
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
			Table x = new Table(strTableName);
			if (x.getAllPages().size() == 0) {
				x.createPage();
				System.out.println("YAAAAY First page intialized");
			}

			String lastPage = x.getLastPage();

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
				// Page lastPageinTable = (Page) deserialize(path);
				lastPageinTable = (Page) loadFileDyn(path);
				if (!lastPageinTable.isFull()) {
					lastPageinTable.addRecord(htblColNameValue);
					lastPageinTable.setRowsCounter(lastPageinTable
							.getRowsCounter() + 1);
				} else {
					x.createPage(); // already added in the method to the array
					lastPage = x.getAllPages().get(x.getAllPages().size() - 1);
					System.out.println("New page created HOHOHOHO");
					path = "data/tables/" + strTableName + "/" + "pages/"
							+ lastPage + ".class";
					// lastPageinTable = (Page) deserialize(path);
					lastPageinTable = (Page) loadFileDyn(path);
					lastPageinTable.addRecord(htblColNameValue);
					lastPageinTable.setRowsCounter(lastPageinTable
							.getRowsCounter() + 1);

				}
			}
			currentPagepath = path;
			// path = "data/tables/" + strTableName + "/" + strTableName +
			// ".bin";
			// serialize(path, x);
			// virtualDirectory.put(path, x);
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
				Comparable O = (Comparable) getValueIfValid(strTableName,
						index, value);
				B.put(O, currentPagepath);
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
		Iterator I = selectValueFromTable(strTableName, htblColNameValue,
				strOperator);
		String Tpath = "data/tables/" + strTableName + "/" + strTableName
				+ ".bin";
		// Table T = (Table) deserialize(Tpath);
		// Table T = (Table) loadFileDyn(Tpath);
		Table T = new Table(strTableName);
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
				while (indicesI.hasNext()) {
					String Column = (String) indicesI.next();
					BTreePath = "data/tables/" + strTableName + "/" + "BTree/"
							+ Column + ".bin";
					LHTPath = "data/tables/" + strTableName + "/"
							+ "hashtable/" + Column + ".bin";
					B = (BTree) loadFileDyn(BTreePath);
					// LinearHashtable L = (LinearHashtable)
					// deserialize(LHTPath);
					L = (LinearHashtable) loadFileDyn(LHTPath);
					L.delete(r.get(Column));
					Comparable O = (Comparable) getValueIfValid(strTableName,
							Column, r.get(Column));
					B.delete(O);
					virtualDirectory.put(LHTPath, L);
					virtualDirectory.put(BTreePath, B);
				}
			}

		}

	}

	public static ArrayList SelectRangeOneCondition(String strTable,
			String ColumnName, String ColumnAllValue)
			throws ClassNotFoundException, IOException {
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		ArrayList result = new ArrayList();
		// Table T = (Table) deserialize(tablepath);
		Table T = new Table(strTable);
		String ColumnValue;
		String Columnrange = getOperator(ColumnAllValue);
		if (Columnrange.length() == 1) {
			ColumnValue = ColumnAllValue.substring(1);
		} else {
			ColumnValue = ColumnAllValue.substring(2);
		}

		if (T.getIndexes().contains(ColumnName)) {
			String BTreePath = "data/tables/" + strTable + "/" + "BTree/"
					+ ColumnName + ".bin";
			// BTree B = (BTree) deserialize(BTreePath);
			BTree B = (BTree) loadFileDyn(BTreePath);
			ArrayList tempo = new ArrayList();
			ArrayList tempoe = new ArrayList();
			ArrayList pathes = new ArrayList();
			ArrayList pathesB = new ArrayList();

			if (Columnrange.charAt(0) == '>') {
				Comparable O = (Comparable) getValueIfValid(strTable,
						ColumnName, ColumnValue);
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
				Comparable O = (Comparable) getValueIfValid(strTable,
						ColumnName, ColumnValue);
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
			Comparable O = (Comparable) getValueIfValid(strTable, ColumnName,
					ColumnValue);
			if (Columnrange.length() != 1 && B.search(O) != null) {

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
				String PagePath = "data/tables/" + strTable + "/" + "pages/"
						+ Pname + ".class";
				// Page p = (Page) deserialize(PagePath);
				Page p = (Page) loadFileDyn(PagePath);
				if (Columnrange.length() != 1)
					tempoe = p.getRecords(ColumnName, ColumnValue);

				if (Columnrange.charAt(0) == '>')
					tempo = p.getRecordbiggerthan(strTable, ColumnName,
							ColumnValue);

				if (Columnrange.charAt(0) == '<')

					tempo = p.getRecordLessthan(strTable, ColumnName,
							ColumnValue);

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
		// Table T = (Table) loadFileDyn(tablepath);
		Table T = new Table(strTable);
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

	public static Iterator selectRangeFromTable(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws ClassNotFoundException, IOException {
		ArrayList result = new ArrayList();
		ArrayList tempresult = new ArrayList();
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		// Table T = (Table) deserialize(tablepath);
		Table T = new Table(strTable);
		Iterator coloumnsI = htblColNameValue.keySet().iterator();
		if (strOperator.equals("OR")) {
			while (coloumnsI.hasNext()) {
				String ColumnName = (String) coloumnsI.next();
				System.out.println(ColumnName); // major
				String ColumnValue = htblColNameValue.get(ColumnName);
				tempresult = (SelectRangeOneCondition(strTable, ColumnName,
						ColumnValue));
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
				String ColumnAllValue = htblColNameValue.get(ColumnName);
				System.out.println("Oo " + ColumnAllValue);
				if (flag == false) {
					tempresult = (SelectRangeOneCondition(strTable, ColumnName,
							ColumnAllValue));
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
						String Columnrange = getOperator(ColumnAllValue);
						String ColumnValue;
						if (Columnrange.length() == 1) {
							ColumnValue = ColumnAllValue.substring(1);
						} else {
							ColumnValue = ColumnAllValue.substring(2);
						}
						Comparable O1 = (Comparable) getValueIfValid(strTable,
								ColumnName, ColumnValue);
						Comparable O2 = (Comparable) getValueIfValid(strTable,
								ColumnName, Record.get(ColumnName));
						if (Columnrange.length() == 2) {
							if (O2.compareTo(O1) != 0)
								if (Columnrange.charAt(0) == '>') {
									if (O2.compareTo(O1) < 0)
										result.remove(Record);
								}
							if (Columnrange.charAt(0) == '<') {
								if (O2.compareTo(O1) > 0)
									result.remove(Record);
							}
						} else {
							if (Columnrange.charAt(0) == '>') {
								if (O2.compareTo(O1) <= 0)
									result.remove(Record);
							}
							if (Columnrange.charAt(0) == '<') {
								if (O2.compareTo(O1) >= 0)
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

	public static Iterator selectValueFromTable(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException, ClassNotFoundException, IOException {
		ArrayList result = new ArrayList();
		ArrayList tempresult = new ArrayList();
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		// Table T = (Table) deserialize(tablepath);
		// Table T = (Table) loadFileDyn(tablepath);
		Table T = new Table(strTable);
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
		String[] br = ((String) readMetaFromDisk()).split("\n");
		for (String line : br) {
			String[] result = line.split(", ");
			if (result[0].equals(strTableName)) {
				original.put(result[1], result[2]);
				// System.out.println(result[1] + ": " + result[2]);
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
			// System.out.println(strColType);
			String strColValue = (String) entry.getValue();
			// System.out.println(strColValue);
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
			// System.out.println(y);

		}

		// TO BE REMOVED
		return true;

	}

	public static Object getValueIfValid(String tableName, String columnName,
			String value) throws IOException, ClassNotFoundException {
		Hashtable<String, String> original = new Hashtable<String, String>();
		String[] br = ((String) readMetaFromDisk()).split("\n");
		for (String line : br) {
			String[] result = line.split(", ");
			if (result[0].equals(tableName)) {
				original.put(result[1], result[2]);
				// System.out.println(result[1] + ": " + result[2]);
			}
		}

		if (!original.containsKey(columnName)) {
			return null;
		}

		String strColType = original.get(columnName);
		System.out.println("type: " + strColType);
		String strColValue = value;
		System.out.println("value: " + strColValue);
		Class x = Class.forName(strColType);
		// System.out.println(x);
		// Constructor conh structor = x.;

		Object y = null;
		try {
			y = x.getDeclaredConstructor(String.class).newInstance(strColValue);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			System.out.println("Invalid input");
			return null;

		}
		System.out.println("returned value: " + y.toString());
		return y;
	}
}

// createTable Done
// createInsex Done
// insertIntoTable Done --> lssa 7war el key bs
// Edit already exists and call it in create table -----> Kareem
// Edit is valid input and call it in create table -----> Kareem
// Edit get value if valid and call it in create table -----> Kareem
// Look at last point in pdf

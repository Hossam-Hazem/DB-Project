import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.sql.Savepoint;
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

	private static void init() {
		virtualDirectory = new Hashtable<String, Object>();
	}

	public static void saveAll() throws DBEngineException, IOException {

		Enumeration ColNames = virtualDirectory.keys();
		while (ColNames.hasMoreElements()) {
			String ColName = (String) ColNames.nextElement();
			serialize(ColName, virtualDirectory.get(ColName));
		}

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
								: "false") + ", " + "false" + ", "
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
					+ i;
			// Page P = (Page) deserialize(Pagepath);
			Page P = (Page) loadFileDyn(Pagepath);

			ArrayList<Hashtable<String, String>> AllRecords = P.getRecords();
			for (int j = 0; j < P.getRowsCounter(); j++) {
				Hashtable<String, String> r = AllRecords.get(j);
				String c = r.get(strColName);
				B.put(c, Pagepath);
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
			path = "data/tables/" + strTableName + "/" + "pages/" + lastPage
					+ ".class";
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
			String BTreePath = "data/tables/" + strTableName + "/" + "BTree/"
					+ index + ".bin";
			String LHTPath = "data/tables/" + strTableName + "/" + "hashtable/"
					+ index + ".bin";
			// BTree B = (BTree) deserialize(BTreePath);
			BTree B = (BTree) loadFileDyn(BTreePath);
			// LinearHashtable L = (LinearHashtable) deserialize(LHTPath);
			LinearHashtable L = (LinearHashtable) loadFileDyn(LHTPath);
			String value = htblColNameValue.get(index);
			B.put(value, currentPagepath);
			L.put(value, currentPagepath);
			// serialize(LHTPath, L);
			virtualDirectory.put(LHTPath, L);
			// serialize(BTreePath, B);
			virtualDirectory.put(BTreePath, B);
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
			String PrimaryKeyValue = r.get(PrimaryKeyColumn);
			String RPath = (String) L.get(PrimaryKeyValue);
			// Page p = (Page) deserialize(RPath);
			Page p = (Page) loadFileDyn(RPath);
			p.removeRecord(PrimaryKeyColumn, PrimaryKeyValue);
			L.delete(PrimaryKeyValue);
			B.delete(PrimaryKeyValue);
			// serialize(RPath, p);
			virtualDirectory.put(RPath, p);
			// serialize(LHTPath, L);
			virtualDirectory.put(LHTPath, L);
			// serialize(BTreePath, B);
			virtualDirectory.put(BTreePath, B);
		}

	}

	public static Iterator selectValueFromTable(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException, ClassNotFoundException, IOException {
		ArrayList result = new ArrayList();
		ArrayList TakenRecords = new ArrayList();
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		// Table T = (Table) deserialize(tablepath);
		Table T = (Table) loadFileDyn(tablepath);
		Iterator coloumnsI = htblColNameValue.keySet().iterator();
		if (strOperator.equals("OR")) {
			while (coloumnsI.hasNext()) {
				String ColumnName = (String) coloumnsI.next();
				System.out.println(ColumnName); // major
				String ColumnValue = htblColNameValue.get(ColumnName);
				if (T.getIndexes().contains(ColumnName)) {
					String LHTPath = "data/tables/" + strTable + "/"
							+ "hashtable/" + ColumnName + ".bin";
					// LinearHashtable L = (LinearHashtable)
					// deserialize(LHTPath);
					LinearHashtable L = (LinearHashtable) loadFileDyn(LHTPath);
					String RecordPath = (String) L.get(ColumnValue);
					if (RecordPath != null) {
						// Page p = (Page) deserialize(RecordPath);
						Page p = (Page) loadFileDyn(RecordPath);
						Hashtable<String, String> r = p.getRecord(ColumnName,
								ColumnValue);
						if (!TakenRecords.contains(p.getPageName()
								+ p.getrecordPlace(r))) {// check if the record
															// isnt already
															// selected
							result.add(r);
							TakenRecords.add(r);
						}
					}

				} else {
					Iterator PagesI = T.getAllPages().iterator();
					while (PagesI.hasNext()) {
						String Pname = (String) PagesI.next();
						String PagePath = "data/tables/" + strTable + "/"
								+ "pages/" + Pname + ".class";
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
								System.out.println("while in OR works");
							}
						}
						/*
						 * if (r != null) if
						 * (!TakenRecords.contains(p.getPageName() +
						 * p.getrecordPlace(r))) {// check if the // record isnt
						 * // already // selected result.add(r);
						 * TakenRecords.add(r); }
						 */
					}
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
					if (T.getIndexes().contains(ColumnName)) {
						String LHTPath = "data/tables/" + strTable + "/"
								+ "hashtable/" + ColumnName + ".bin";
						// LinearHashtable L = (LinearHashtable)
						// deserialize(LHTPath);
						LinearHashtable L = (LinearHashtable) loadFileDyn(LHTPath);
						String RecordPath = (String) L.get(ColumnValue);
						if (RecordPath != null) {
							// Page p = (Page) deserialize(RecordPath);
							Page p = (Page) loadFileDyn(RecordPath);
							Hashtable<String, String> r = p.getRecord(
									ColumnName, ColumnValue);
							result.add(r);

						}

					} else {
						Iterator PagesI = T.getAllPages().iterator();
						while (PagesI.hasNext()) {
							String Pname = (String) PagesI.next();
							String PagePath = "data/tables/" + strTable + "/"
									+ "pages/" + Pname + ".class";
							// Page p = (Page) deserialize(PagePath);
							Page p = (Page) loadFileDyn(PagePath);
							Hashtable<String, String> r = p.getRecord(
									ColumnName, ColumnValue);
							ArrayList<Hashtable<String, String>> allRecordsInPage = p
									.getRecords();
							for (int i = 0; i < allRecordsInPage.size(); i++) {
								if (allRecordsInPage.get(i).get(ColumnName)
										.equals(ColumnValue)) {
									result.add(allRecordsInPage.get(i));
									System.out.println("while in OR works");
								}
							}
							/*
							 * if (r != null) result.add(r);
							 * 
							 * System.out.println(r.get("name"));
							 */

						}
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

	public static Iterator selectRangeFromTable(String strTable,
			Hashtable<String, String> htblColNameRange, String strOperator)
			throws DBEngineException, ClassNotFoundException, IOException {
		ArrayList result = new ArrayList();
		ArrayList TakenRecords = new ArrayList();
		String tablepath = "data/tables/" + strTable + "/" + strTable + ".bin";
		// Table T = (Table) deserialize(tablepath);
		Table T = (Table) loadFileDyn(tablepath);
		Iterator coloumnsI = htblColNameRange.keySet().iterator();
		if (strOperator.equals("OR")) {
			while (coloumnsI.hasNext()) {
				String ColumnName = (String) coloumnsI.next();
				System.out.println(ColumnName); // major
				String ColumnAllValue = htblColNameRange.get(ColumnName);
				String Columnrange = getOperator(ColumnAllValue);
				String ColumnValue;
				if (Columnrange.length() == 1) {
					ColumnValue = ColumnAllValue.substring(1);
				} else {
					ColumnValue = ColumnAllValue.substring(2);
				}
				if (Columnrange.length() == 1)// low kan one m3nah la greater
												// than la smaller than
					ColumnValue = ColumnAllValue.substring(1);
				else
					ColumnValue = ColumnAllValue.substring(2);
				if (T.getIndexes().contains(ColumnName)) {
					String BTreePath = "data/tables/" + strTable + "/"
							+ "BTree/" + ColumnName + ".bin";
					// BTree B = (BTree) deserialize(BTreePath);
					BTree B = (BTree) loadFileDyn(BTreePath);
					ArrayList tempo = new ArrayList();
					ArrayList tempoe = new ArrayList();
					ArrayList pathes = new ArrayList();
					ArrayList pathesB = new ArrayList();
<<<<<<< HEAD
					if (B.search(ColumnValue) != null) {

						if (Columnrange.charAt(0) == '>') {
							pathes = B.getbiggerthan(ColumnValue);
							Iterator pathesI = pathes.iterator();
							ArrayList PagesScanned = new ArrayList();
							while (pathesI.hasNext()) {
								String PagePath = (String) pathesI.next();
								if (!PagesScanned.contains(PagePath)) {
									// Page p = (Page) deserialize((PagePath));
									Page p = (Page) loadFileDyn((PagePath));
									Iterator Itemp = p.getRecordbiggerthan(
											ColumnName, ColumnValue).iterator();
									while (Itemp.hasNext()) {
										Hashtable<String, String> r = (Hashtable<String, String>) Itemp
												.next();
										if (!TakenRecords.contains(p
												.getPageName()
												+ p.getrecordPlace(r))) {
											result.add(r);
											TakenRecords.add(p.getPageName()
													+ p.getrecordPlace(r));
										}
									}
=======
>>>>>>> origin/Hossam

					if (Columnrange.charAt(0) == '>') {
						pathes = B.getbiggerthan(ColumnValue);
						Iterator pathesI = pathes.iterator();
						ArrayList PagesScanned = new ArrayList();
						while (pathesI.hasNext()) {
							String PagePath = (String) pathesI.next();
							if (!PagesScanned.contains(PagePath)) {
								Page p = (Page) deserialize((PagePath));
								Iterator Itemp = p.getRecordbiggerthan(
										ColumnName, ColumnValue).iterator();
								while (Itemp.hasNext()) {
									Hashtable<String, String> r = (Hashtable<String, String>) Itemp
											.next();
									if (!TakenRecords.contains(p.getPageName()
											+ p.getrecordPlace(r))) {
										result.add(r);
										TakenRecords.add(p.getPageName()
												+ p.getrecordPlace(r));
									}
								}
								PagesScanned.add(PagePath);
							}

						}
					}

<<<<<<< HEAD
						if (Columnrange.charAt(0) == '<') {
							pathes = B.getSmallerthan(ColumnValue);
							Iterator pathesI = pathes.iterator();
							ArrayList PagesScanned = new ArrayList();
							while (pathesI.hasNext()) {
								String PagePath = (String) pathesI.next();
								if (!PagesScanned.contains(PagePath)) {
									// Page p = (Page) deserialize((PagePath));
									Page p = (Page) loadFileDyn((PagePath));
									Iterator Itemp = p.getRecordbiggerthan(
											ColumnName, ColumnValue).iterator();
									while (Itemp.hasNext()) {
										Hashtable<String, String> r = (Hashtable<String, String>) Itemp
												.next();
										if (!TakenRecords.contains(p
												.getPageName()
												+ p.getrecordPlace(r))) {
											result.add(r);
											TakenRecords.add(p.getPageName()
													+ p.getrecordPlace(r));

										}
									}
=======
					if (Columnrange.charAt(0) == '<') {
						pathes = B.getSmallerthan(ColumnValue);
						Iterator pathesI = pathes.iterator();
						ArrayList PagesScanned = new ArrayList();
						while (pathesI.hasNext()) {
							String PagePath = (String) pathesI.next();
							if (!PagesScanned.contains(PagePath)) {
								Page p = (Page) deserialize((PagePath));
								Iterator Itemp = p.getRecordbiggerthan(
										ColumnName, ColumnValue).iterator();
								while (Itemp.hasNext()) {
									Hashtable<String, String> r = (Hashtable<String, String>) Itemp
											.next();
									if (!TakenRecords.contains(p.getPageName()
											+ p.getrecordPlace(r))) {
										result.add(r);
										TakenRecords.add(p.getPageName()
												+ p.getrecordPlace(r));
>>>>>>> origin/Hossam

									}
								}
								PagesScanned.add(PagePath);

							}

						}
					}

					if (Columnrange.length() != 1&&B.search(ColumnValue)!=null) {
						
						String path = (String) B.search(ColumnValue);
						Page p = (Page) deserialize((path));
						Hashtable<String, String> r = (Hashtable<String, String>) p
								.getRecord(ColumnName, ColumnValue);
						if (!TakenRecords.contains(p.getPageName()
								+ p.getrecordPlace(r))) {
							result.add(r);
							TakenRecords.add(p.getPageName()
									+ p.getrecordPlace(r));
						}

<<<<<<< HEAD
						if (Columnrange.length() != 1) {
							String path = (String) B.search(ColumnValue);
							// Page p = (Page) deserialize((path));
							Page p = (Page) loadFileDyn((path));
							Hashtable<String, String> r = (Hashtable<String, String>) p
									.getRecord(ColumnName, ColumnValue);
=======
					}

				} else {
					Iterator PagesI = T.getAllPages().iterator();
					while (PagesI.hasNext()) {
						ArrayList tempo = new ArrayList();
						ArrayList tempoe = new ArrayList();
						String Pname = (String) PagesI.next();
						String PagePath = "data/tables/" + strTable + "/"
								+ "pages/" + Pname + ".class";
						Page p = (Page) deserialize(PagePath);
						if (Columnrange.length() != 1)
							tempoe = p.getRecords(ColumnName, ColumnValue);

						if (Columnrange.charAt(0) == '>')
							tempo = p.getRecordbiggerthan(ColumnName,
									ColumnValue);

						if (Columnrange.charAt(0) == '<')
							tempo = p
									.getRecordLessthan(ColumnName, ColumnValue);

						Iterator tempoI = tempoe.iterator();
						while (tempoI.hasNext()) {
							Hashtable<String, String> r = (Hashtable<String, String>) tempoI
									.next();
>>>>>>> origin/Hossam
							if (!TakenRecords.contains(p.getPageName()
									+ p.getrecordPlace(r))) {
								result.add(r);
								TakenRecords.add(p.getPageName()
										+ p.getrecordPlace(r));
							}

						}
<<<<<<< HEAD

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
							tempo = p.getRecordbiggerthan(ColumnName,
									ColumnValue);

						if (Columnrange.charAt(0) == '<')
							tempo = p
									.getRecordLessthan(ColumnName, ColumnValue);

						Iterator tempoI = tempoe.iterator();
=======
						tempoI = tempo.iterator();
>>>>>>> origin/Hossam
						while (tempoI.hasNext()) {
							Hashtable<String, String> r = (Hashtable<String, String>) tempoI
									.next();
							if (!TakenRecords.contains(p.getPageName()
									+ p.getrecordPlace(r))) {
								result.add(r);
								TakenRecords.add(p.getPageName()
										+ p.getrecordPlace(r));
							}
<<<<<<< HEAD

						}
						tempoI = tempo.iterator();
						while (tempoI.hasNext()) {
							Hashtable<String, String> r = (Hashtable<String, String>) tempoI
									.next();
							if (!TakenRecords.contains(p.getPageName()
									+ p.getrecordPlace(r))) {
								result.add(r);
								TakenRecords.add(p.getPageName()
										+ p.getrecordPlace(r));
							}
						}
=======
						}
>>>>>>> origin/Hossam

						/*
						 * if (r != null) if
						 * (!TakenRecords.contains(p.getPageName() +
						 * p.getrecordPlace(r))) {// check if the // record isnt
						 * // already // selected result.add(r);
						 * TakenRecords.add(r); }
						 */
					}

				}

				for (int i = 0; i < result.size(); i++) {
					System.out.println(result.get(i));
				}
				return result.iterator();
			}
		}
		/*
		 * if (strOperator.equals("AND")) { boolean flag = false; while
		 * (coloumnsI.hasNext()) { String ColumnName = (String)
		 * coloumnsI.next(); String ColumnValue =
		 * htblColNameValue.get(ColumnName); if (flag == false) { if
		 * (T.getIndexes().contains(ColumnName)) { String LHTPath =
		 * "data/tables/" + strTable + "/" + "hashtable/" + ColumnName + ".bin";
		 * LinearHashtable L = (LinearHashtable) deserialize(LHTPath); String
		 * RecordPath = (String) L.get(ColumnValue); if (RecordPath != null) {
		 * Page p = (Page) deserialize(RecordPath); Hashtable<String, String> r
		 * = p.getRecord( ColumnName, ColumnValue); result.add(r);
		 * 
		 * }
		 * 
		 * } else { Iterator PagesI = T.getAllPages().iterator(); while
		 * (PagesI.hasNext()) { String Pname = (String) PagesI.next(); String
		 * PagePath = "data/tables/" + strTable + "/" + "pages/" + Pname +
		 * ".class"; Page p = (Page) deserialize(PagePath); Hashtable<String,
		 * String> r = p.getRecord( ColumnName, ColumnValue);
		 * ArrayList<Hashtable<String, String>> allRecordsInPage = p
		 * .getRecords(); for (int i = 0; i < allRecordsInPage.size(); i++) { if
		 * (allRecordsInPage.get(i).get(ColumnName) .equals(ColumnValue)) {
		 * result.add(allRecordsInPage.get(i));
		 * System.out.println("while in OR works"); } } // // if (r != null)
		 * result.add(r); // //* System.out.println(r.get("name")); //*
		 * 
		 * } }
		 * 
		 * flag = true; } else { // low flag mesh be false i.e. mesh 2wl
		 * iteration // 3lshan yeloop 3ala result mesh database // 3lshan // AND
		 * kan nefse 23mlha recursion <3
		 * 
		 * ArrayList temp = new ArrayList(); temp = (ArrayList) result.clone();
		 * Iterator ResultI = temp.iterator(); while (ResultI.hasNext()) {
		 * Hashtable<String, String> Record = (Hashtable<String, String>)
		 * ResultI .next(); if (!Record.get(ColumnName).equals(ColumnValue))
		 * result.remove(Record);
		 * 
		 * } }
		 * 
		 * } for (int i = 0; i < result.size(); i++) {
		 * System.out.println(result.get(i)); }
		 * 
		 * return result.iterator(); }
		 */

		return null;

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

	public static void main(String[] args) throws IOException, DBAppException,
			ClassNotFoundException, DBEngineException {

		// test save all by doing the following : go to ===>

		init();

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("col1", "str");
		htblColNameType.put("col2", "int");
		htblColNameType.put("col3", "int");
		htblColNameType.put("col4", "str");

		Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();
		htblColNameRefs.put("col1", "table1.id");

		// ===> execute once and comment createTable and execute multiple times
		createTable("testAll", htblColNameType, htblColNameRefs, "col2");

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

		for (int i = 0; i < 200; i++) {
			Hashtable<String, String> insertion = new Hashtable<String, String>();
			insertion.put("col1", "str");
			insertion.put("col2", "int");
			insertion.put("col3", "int");
			insertion.put("col4", "str");

			insertIntoTable("testAll", insertion);
		}

		saveAll();

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
<<<<<<< HEAD
		/*
		 * // Page p = (Page)
		 * deserialize("data/tables/testrangeor/Pages/0.class"); Page p = (Page)
		 * loadFileDyn("data/tables/testrangeor/Pages/0.class");
		 * System.out.println("All Records: " + p.getRecords());
		 * 
		 * // BTree x = (BTree) //
		 * deserialize("data/tables/testrangeor/BTree/ID.bin"); BTree x =
		 * (BTree) loadFileDyn("data/tables/testrangeor/BTree/ID.bin");
		 * x.print();
		 * 
		 * Hashtable<String, String> htblColNameRange = new Hashtable<String,
		 * String>(); htblColNameRange.put("age", ">=0");
		 * htblColNameRange.put("ID", ">286205"); //
		 * htblColNameValue.put("name", "hossam"); Iterator I =
		 * selectRangeFromTable("testrangeor", htblColNameRange, "OR");
		 * 
		 * while (I.hasNext()) { System.out.println("done " +
		 * I.next().toString()); }
		 */
=======

		//Page p = (Page) deserialize("data/tables/testrangeor/Pages/0.class");
		//System.out.println("All Records: " + p.getRecords());

		//BTree x = (BTree) deserialize("data/tables/testrangeor/BTree/age.bin");
		//x.print();
		createIndex("testrangeor1", "age");
		Hashtable<String, String> htblColNameRange = new Hashtable<String, String>();
		 htblColNameRange.put("age", "<=3");
		//htblColNameRange.put("age", "<=6");
		// htblColNameValue.put("name", "hossam");
		Iterator I = selectRangeFromTable("testrangeor1", htblColNameRange, "OR");
		
		while (I.hasNext()) {
			System.out.println("done " + I.next().toString());
		}

>>>>>>> origin/Hossam
	}
}

// createTable Done
// createInsex Done
// insertIntoTable Done --> lssa 7war el key bs

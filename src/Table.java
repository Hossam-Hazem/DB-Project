import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;

import BPTree.BTree;

public class Table implements Serializable {
	private ArrayList<String> allPages;
	private ArrayList<String> Indexes;
	private int nameCounter;
	private String pagesDirectory;
	private String indexesDirectory;
	private String tableName;
	private String tablePath;
	private String lastPage;

	private static final long serialVersionUID = 1928828356132285922L;

	public Table(String tableNameToBeCreated, String PrimaryKey)
			throws IOException {
		this.Indexes = new ArrayList<String>();
		this.tableName = tableNameToBeCreated;
		this.pagesDirectory = "data/tables/" + tableName + "/pages";
		this.indexesDirectory = "data/tables/" + tableName + "/hashtable";
		this.tablePath = "data/tables/" + tableName;

		makeTableFolders(PrimaryKey);

		this.allPages = getPages();
		this.nameCounter = getNameCounter(this.allPages);
		this.Indexes = getAllIndexes();
	}

	public Table(String tableNameToBeRetrieved) throws IOException {
		this.Indexes = new ArrayList<String>();
		this.tableName = tableNameToBeRetrieved;
		this.pagesDirectory = "data/tables/" + tableName + "/pages";
		this.indexesDirectory = "data/tables/" + tableName + "/hashtable";
		this.tablePath = "data/tables/" + tableName;

		this.allPages = getPages();
		this.nameCounter = getNameCounter(this.allPages);
		this.Indexes = getAllIndexes();
	}

	private void makeTableFolders(String PrimaryKey) throws IOException {
		String path = "data/tables/" + tableName + "/" + tableName + ".bin";
		// make folder containing all table info
		File saveDir = new File("data/tables");
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		// make pages directory inside table folder
		saveDir = new File("data/tables/" + tableName + "/" + "pages");
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		// make hashtable directory inside table folder
		saveDir = new File("data/tables/" + tableName + "/" + "hashtable");
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		// make BTree directory inside table folder
		saveDir = new File("data/tables/" + tableName + "/" + "BTree");
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		//DBApp.virtualDirectory.put(path, this);
		BTree T = new BTree();
		path = "data/tables/" + tableName + "/BTree/" + PrimaryKey + ".bin";
		// serialize(path, T);
		DBApp.virtualDirectory.put(path, T);
		LinearHashtable H = new LinearHashtable();
		path = "data/tables/" + tableName + "/hashtable/" + PrimaryKey + ".bin";
		// serialize(path, H);
		DBApp.virtualDirectory.put(path, H);
	}

	public void createPage() throws IOException {
		Page x = new Page("" + nameCounter);
		String path = pagesDirectory + "/" + x.getPageName() + ".class";
		/*
		 * FileOutputStream fs = new FileOutputStream(path); ObjectOutputStream
		 * os = new ObjectOutputStream(fs); os.writeObject(x); os.close();
		 * fs.close();
		 */
		DBApp.virtualDirectory.put(path, x);
		allPages.add("" + nameCounter);
		lastPage = "" + nameCounter;
		nameCounter++;

	}

	public ArrayList<String> getPages() {

		ArrayList<String> pages = new ArrayList<>();

		Enumeration dirs = DBApp.virtualDirectory.keys();
		while (dirs.hasMoreElements()) {
			String dirName = (String) dirs.nextElement();
			if (dirName.indexOf(pagesDirectory) == 0) {
				String tmp = dirName.replace(pagesDirectory + "/", "");
				pages.add(tmp.substring(0, tmp.length() - 6));
			}
		}

		// pagesDirectory;
		File file = new File(pagesDirectory);
		if (file.exists() && file.isDirectory()) {
			File[] list = file.listFiles();
			// for each item in the list
			for (File file1 : list) {
				String fileName = file1.getName().substring(0,
						file1.getName().length() - 6);
				if (file1.isFile() && (!pages.contains(fileName))) {
					pages.add(fileName);
				}
			}

		}
		return pages;
	}

	public ArrayList<String> getAllIndexes() {
		ArrayList<String> Indexes = new ArrayList<>();

		Enumeration dirs = DBApp.virtualDirectory.keys();
		while (dirs.hasMoreElements()) {
			String dirName = (String) dirs.nextElement();
			if (dirName.indexOf(indexesDirectory) == 0) {
				String tmp = dirName.replace(indexesDirectory + "/", "");
				Indexes.add(tmp.substring(0, tmp.length() - 4));
			}
		}

		File file = new File(indexesDirectory);
		if (file.exists() && file.isDirectory()) {
			File[] list = file.listFiles();
			// for each item in the list
			for (File file1 : list) {
				String indexName = file1.getName().substring(0,
						file1.getName().length() - 4);
				if (file1.isFile() && (!Indexes.contains(indexName))) {
					Indexes.add(indexName);
				}
			}
		}
		return Indexes;
	}

	public int getNameCounter(ArrayList<String> pages) throws IOException {
		int nameCounter = 0;
		if (pages.size() > 0) {
			lastPage = pages.get(0);
			for (int i = 0; i < pages.size(); i++) {
				if (Integer.parseInt(pages.get(i).substring(0)
						.replace(".class", "")) > Integer.parseInt(lastPage
						.substring(0).replace(".class", ""))) {
					lastPage = pages.get(i);
				}
			}
			nameCounter = Integer.parseInt(lastPage.substring(0).replace(
					".class", ""));
			nameCounter++;
		}
		return nameCounter;
	}

	public ArrayList<String> getAllPages() {
		return allPages;
	}

	public ArrayList<String> getIndexes() {
		return Indexes;
	}

	public int getNameCounter() {
		return nameCounter;
	}

	public void setNameCounter(int nameCounter) {
		this.nameCounter = nameCounter;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public String getPagesDirectory() {
		return pagesDirectory;
	}

	public void setPagesDirectory(String pagesDirectory) {
		this.pagesDirectory = pagesDirectory;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void addPagetoArray(Page x) {
		allPages.add(x.getPageName());
	}

	public void addIndextoArray(String x) {
		Indexes.add(x);
	}

	public String getLastPage() {
		return lastPage;
	}

	public void setLastPage(String lastPage) {
		this.lastPage = lastPage;
	}

	public static void main(String[] args) throws IOException {
		// new Table("Test2");
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
		System.out.println("");
	}
}
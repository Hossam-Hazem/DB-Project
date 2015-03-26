import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import BPTree.BTree;

public class Table implements Serializable {
	private ArrayList<String> allPages;
	private ArrayList<String> Indexes;
	private int nameCounter;
	private String pagesDirectory;
	private String tableName;

	private static final long serialVersionUID = 1928828356132285922L;

	public Table(String name, String PrimaryKey) throws IOException {
		this.allPages = new ArrayList<String>();
		this.Indexes = new ArrayList<String>();
		this.tableName = name;
		this.nameCounter = 0;
		Indexes.add(PrimaryKey);
		pagesDirectory = "data/tables/" + tableName + "/pages";
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
		/*
		 * FileOutputStream fs = new FileOutputStream(path); ObjectOutputStream
		 * os = new ObjectOutputStream(fs); os.writeObject(x); os.close();
		 * fs.close();
		 */
		// serialize(path, this);
		DBApp.virtualDirectory.put(path, this);
		BTree T = new BTree();
		path = "data/tables/" + tableName + "/BTree/" + PrimaryKey + ".bin";
		// serialize(path, T);
		DBApp.virtualDirectory.put(path, T);
		LinearHashtable H = new LinearHashtable();
		path = "data/tables/" + tableName + "/hashtable/" + PrimaryKey + ".bin";
		// serialize(path, H);
		DBApp.virtualDirectory.put(path, H);

	}

	/*
	 * public void createPage() throws IOException { Page x = new
	 * Page(tableName, "" + pagesNames.size()); File saveDir = new File("data/"
	 * + tableName); if (!saveDir.exists()) { saveDir.mkdirs(); }
	 * FileOutputStream fs = new FileOutputStream("data/" + tableName + "/" +
	 * pagesNames.size() + ".bin"); ObjectOutputStream os = new
	 * ObjectOutputStream(fs); os.writeObject(x); os.close(); fs.close(); //
	 * pagesNames.add(x.); }
	 */

	public void createPage() throws IOException {
		Page x = new Page("" + nameCounter);
		String path = pagesDirectory + "/" + x.getPageName() + ".class";
		/*
		FileOutputStream fs = new FileOutputStream(path);
		ObjectOutputStream os = new ObjectOutputStream(fs);
		os.writeObject(x);
		os.close();
		fs.close();
		*/
		DBApp.virtualDirectory.put(path, x);
		allPages.add("" + nameCounter);
		nameCounter++;

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
	}

}
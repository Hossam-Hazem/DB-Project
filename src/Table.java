import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class Table implements Serializable {
	private ArrayList<String> allPages;
	private int nameCounter;
	private String pagesDirectory;
	private String tablePath;
	private String tableName;

	private static final long serialVersionUID = 1928828356132285922L;

	public Table(String name) throws IOException {
		this.tableName = name;
		this.pagesDirectory = "data/tables/" + tableName + "/pages";
		this.tablePath = "data/tables/" + tableName;
		this.allPages = getPages();
		this.nameCounter = getNameCounter(this.allPages);
		File file = new File(this.tablePath);
		if (!file.exists()) {
			makeTableFolders();
		}
	}

	public ArrayList<String> getPages() {
		ArrayList<String> pages = new ArrayList<>();
		File file = new File(pagesDirectory);
		if (file.exists() && file.isDirectory()) {
			File[] list = file.listFiles();
			// for each item in the list
			for (File file1 : list) {
				if (file1.isFile()) {
					pages.add(file1.getName());
				}
			}
		}
		return pages;
	}

	public int getNameCounter(ArrayList<String> pages) throws IOException {
		int nameCounter = 0;
		if (pages.size() > 0) {
			String lastPage = pages.get(0);
			for (int i = 0; i < pages.size(); i++) {

				if (Integer.parseInt(pages.get(i).substring(0)
						.replace(".class", "")) > Integer.parseInt(lastPage
						.substring(0).replace(".class", ""))) {
					lastPage = pages.get(i);
				}

				nameCounter = Integer.parseInt(lastPage.substring(0).replace(
						".class", ""));
				nameCounter += Page.getPageSize();
			}

		}
		return nameCounter;
	}

	private void makeTableFolders() throws IOException {
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

		// serialize(path, this);
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
		FileOutputStream fs = new FileOutputStream(path);
		ObjectOutputStream os = new ObjectOutputStream(fs);
		os.writeObject(x);
		os.close();
		fs.close();
		allPages.add("" + nameCounter);
		nameCounter++;

	}

	public ArrayList<String> getAllPages() {
		return allPages;
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

	public String getLastPagePath() {
		return (pagesDirectory + "/" + allPages.get(allPages.size() - 1));
	}
/*
	public static void serialize(String path, Object x) throws IOException {
		FileOutputStream fs = new FileOutputStream(path);
		ObjectOutputStream os = new ObjectOutputStream(fs);
		os.writeObject(x);
		os.close();
		fs.close();
	}
*/
	public static void main(String[] args) throws IOException {
		new Table("Test2");
	}
}
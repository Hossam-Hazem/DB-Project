import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class Table {

	private String name;
	private ArrayList<String> pages = new ArrayList<>();
	private String lastPage;
	private int nameCounter;
	private String tablesDir = "data\\";

	public Table(String n) throws IOException {
		name = n;

		// get the list of all the pages in a table
		File f = new File(tablesDir + n);
		if (f.exists() && f.isDirectory()) {
			ArrayList<String> array = new ArrayList<>();
			File[] list = f.listFiles();
			for (File file : list) {
				if (file.isFile() && file.getName().charAt(0) == 'p') {
					array.add(file.getName());
				}
			}
			if (array.size() > 0) {
				// get last page
				lastPage = array.get(0);
				Iterator itr = array.iterator();
				while (itr.hasNext()) {
					String page = (String) itr.next();
					if (Integer.parseInt(page.substring(1)
							.replace(".class", "")) > Integer.parseInt(lastPage
							.substring(1).replace(".class", ""))) {
						lastPage = page;
					}
				}
				nameCounter = Integer.parseInt(lastPage.substring(1).replace(
						".class", ""));
				nameCounter += 200;
				pages = array;
			}
		}

	}

	public void createPage() throws IOException {
		new Page(tablesDir + name + "/p" + nameCounter,"p" + nameCounter);
		pages.add("p" + nameCounter + ".class");
		nameCounter++;

	}

	public Page readPage(String pageName) throws IOException,
			ClassNotFoundException {
		ObjectInputStream is = new ObjectInputStream(new FileInputStream(
				tablesDir + name + "\\" + pageName));
		return (Page) is.readObject();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ArrayList<String> getPages() {
		return pages;
	}

	public void setPages(ArrayList<String> pages) {
		this.pages = pages;
	}

	public String getLastPage() {
		return lastPage;
	}

	public void setLastPage(String lastPage) {
		this.lastPage = lastPage;
	}

	public int getNameCounter() {
		return nameCounter;
	}

	public void setNameCounter(int nameCounter) {
		this.nameCounter = nameCounter;
	}

	public String getTablesDir() {
		return tablesDir;
	}

	public void setTablesDir(String tablesDir) {
		this.tablesDir = tablesDir;
	}

}
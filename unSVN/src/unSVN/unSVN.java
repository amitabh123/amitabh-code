package unSVN;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *
 * @author Amitabh
 */
public class unSVN {
    public static void main(String[] args) throws IOException {
        puts ("un-SVN. Deletes all .svn directores from a given root folder (inclusive)\n");
        puts ("Be very careful using this.\n");
 	try{
            new unSVN().processdir(args);
        }
        catch (IOException ex){
            puts("Error: "+ex.getMessage()+"\n");
            return;
        }
    }

    public static void puts(String string) {
        System.out.print(string);
    }
    private BufferedReader in;
    private String dir1;
    
    public void getInDir() throws IOException {
        puts("Please enter the directory name to read: ");
        in = new BufferedReader(new InputStreamReader(System.in));
        dir1 = in.readLine();
    }

    public void processdir(String args[]) throws IOException {
        switch (args.length){
		case 0:
                        getInDir();
                default:
        }
        File dir = new File(dir1);
        boolean exist = dir.isDirectory();
        if (!exist){
            puts("Directory does not exist.\n");
            return;
        }
        puts ("unSVN-ing " + dir1+"\n");
        visitAllDirs(dir);
    }
    public static void visitAllDirs(File dir) {
        if (dir.isDirectory()) {
            if (dir.getName().compareTo(".svn")==0) {
                deleteTree(dir);
            } else {
                String[] children = dir.list();
                for (int i=0; i<children.length; i++) {
                    visitAllDirs(new File(dir, children[i]));
                }
            }
        }
    }
    private static void deleteTree(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                deleteTree(new File(dir, children[i]));
            }
        }
        dir.delete();
    }
}

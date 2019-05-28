package traitement;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import entities.Comment;
import entities.Post;
import entities.PostCom;

public class FileReader implements Runnable {

	private BlockingQueue<PostCom> blockingQueue;
	private File file;
	private Scanner sc;
	private boolean postReader;
	private PostCom POISON_PILL = new PostCom(null, (long) -1);
	
	public FileReader(BlockingQueue<PostCom> blockingQueue, File file, boolean postReader) throws FileNotFoundException{
		super();
		this.blockingQueue = blockingQueue;
		this.file = file;
		this.postReader = postReader;
		this.sc = new Scanner(this.file);
	}

	public void PostReader() {
		
		String val;
		String[] lVal;
		Post temp;
		Long idTemp;
		Timestamp tsTemp;
		
		while (this.sc.hasNextLine()) {
			val = this.sc.nextLine();
			if (!val.isEmpty()) {
				lVal = val.split(Pattern.quote("|"));
				idTemp = Long.valueOf(lVal[1]);
				tsTemp = Timestamp.valueOf((lVal[0].split(Pattern.quote("+")))[0].replaceAll("T", " "));
				temp = new Post(tsTemp,idTemp, lVal[4]);
				try {
					this.blockingQueue.put(temp);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void CommentReader() {
		String val;
		String[] lVal;
		Comment temp;
		Long idTemp;		
		Timestamp tsTemp;
		Long idParentTemp;
		
		while (this.sc.hasNextLine()) {
			val = this.sc.nextLine();
			if (!val.isEmpty()) {
				lVal = val.split(Pattern.quote("|"));
				idTemp = Long.valueOf(lVal[1]);
				tsTemp = Timestamp.valueOf((lVal[0].split(Pattern.quote("+")))[0].replaceAll("T", " "));
				if (lVal[5].equals("-1")||lVal[5].equals("")) {
					idParentTemp = Long.valueOf(lVal[6]);
					temp = new Comment(tsTemp, idTemp, idParentTemp, true);
				} else {
					idParentTemp = Long.valueOf(lVal[5]);
					temp = new Comment(tsTemp, idTemp, idParentTemp, false);
				}
				try {
					this.blockingQueue.put(temp);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void run() {
		if (postReader) {
			PostReader();
		} else {
			CommentReader();
		}
		try {
			this.blockingQueue.put(POISON_PILL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.sc.close();
	}

}

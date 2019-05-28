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

public class ReaderSorter implements Runnable{
	private File postFile_;
	private File commentFile_;
	private Scanner postScanner_;
	private Scanner commentScanner_;
	private PostCom POISON_PILL = new PostCom(null, (long) -1);
	private BlockingQueue<PostCom> blockingQueue_;
	
	public ReaderSorter(File post, File comment, BlockingQueue<PostCom> queue) throws FileNotFoundException {
		postFile_ = post;
		commentFile_ = comment;
		
		blockingQueue_ = queue;
		
		postScanner_ = new Scanner(postFile_);
		commentScanner_ = new Scanner(commentFile_);
	}
	
	public Comment CommentReader() {
		String val;
		String[] lVal;
		Comment temp;
		Long idTemp;		
		Timestamp tsTemp;
		Long idParentTemp;
		
		if(commentScanner_.hasNextLine()) {
			val = commentScanner_.nextLine();
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
				return temp;
			}
		}		
		return null;
	}
	
	
	public Post PostReader() {
		
		String val;
		String[] lVal;
		Post temp;
		Long idTemp;
		Timestamp tsTemp;
		
		if(postScanner_.hasNextLine()) {
			val = postScanner_.nextLine();
			if (!val.isEmpty()) {
				lVal = val.split(Pattern.quote("|"));
				idTemp = Long.valueOf(lVal[1]);
				tsTemp = Timestamp.valueOf((lVal[0].split(Pattern.quote("+")))[0].replaceAll("T", " "));
				temp = new Post(tsTemp,idTemp, lVal[4]);
				return temp;
			}
		}
		return null;
	}
	
	public void run() {
		boolean bPost = true;
		boolean bComment = true;
		PostCom currentPost = null;
		PostCom currentComment = null;
		while (bPost || bComment) {
			try {
				if (bPost && bComment) {
					if (currentPost == null) {
						currentPost = PostReader();
					}
					if (currentComment == null) {
						currentComment = CommentReader();
					}
					if (currentPost == null || currentComment == null) {
						if (currentPost == null) {
							bPost = false;
						}
						if (currentComment == null) {
							bComment = false;
						}
					} else {
						if (currentPost.after(currentComment)) {
							blockingQueue_.put(currentComment);
							currentComment = null;
						} else {
							blockingQueue_.put(currentPost);
							currentPost = null;
						}
					}
				} else if (bPost && !bComment) {
					if (currentPost == null) {
						currentPost = PostReader();
					}
					if (currentPost == null) {
						bPost = false;
					} else {
						blockingQueue_.put(currentPost);
						currentPost = null;
					}
				} else {
					if (currentComment == null) {
						currentComment = CommentReader();
					}
					if (currentComment == null) {
						bComment = false;
					} else {
						blockingQueue_.put(currentComment);
						currentComment = null;
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		//poison pile
		try {
			blockingQueue_.put(POISON_PILL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		//fermeture scanner
		postScanner_.close();
		commentScanner_.close();
	}
}

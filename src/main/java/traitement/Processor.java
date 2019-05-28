package traitement;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import entities.Comment;
import entities.Post;
import entities.PostCom;

public class Processor implements Runnable{
	
	private BlockingQueue<PostCom> blockingQueue;
	private Map<Long, Post> posts;
	private Map<Long, Long> dicoComPost;
	
	private Long idFirst = (long) -1;
	private int scoreFirst = -1;
	
	private Long idSecond = (long) -1;
	private int scoreSecond = -1;
	
	private Long idThird = (long) -1;
	private int scoreThird = -1;
	
	private String result = "";
	private boolean display;
		
	public Processor(BlockingQueue<PostCom> blockingQueue, boolean test) {
		super();
		this.blockingQueue = blockingQueue;
		this.posts = new HashMap<Long, Post>();
		this.dicoComPost = new HashMap<Long, Long>();
		this.display = false;
	}
	
	public Processor(BlockingQueue<PostCom> blockingQueue) {
		super();
		this.blockingQueue = blockingQueue;
		this.posts = new HashMap<Long, Post>();
		this.dicoComPost = new HashMap<Long, Long>();
		this.display = true;
	}
	
	public void run() {
		
		//int i = 0;
		
		PostCom currentVal = null;
		
		while (currentVal == null) {
			try {
				currentVal = this.blockingQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		while (!currentVal.isPoison()) {
			
			//System.out.println(i++);
			
			if (currentVal instanceof Post) {
				Post post = (Post) currentVal;
				this.posts.put(post.getId(), post);
			} else {
				Comment com = (Comment) currentVal;
				if (com.isParentPost()) {
					if (this.posts.containsKey(com.getIdParent())) {
						this.dicoComPost.put(com.getId(), com.getIdParent());
						this.posts.get(com.getIdParent()).add(com);
					}
				} else {
					if (this.dicoComPost.containsKey(com.getIdParent())) {
						Long idPostTemp = this.dicoComPost.get(com.getIdParent());
						if (this.posts.containsKey(idPostTemp)) {
							this.dicoComPost.put(com.getId(), idPostTemp);
							this.posts.get(idPostTemp).add(com);
						}
					}
				}
			}
			calcul(currentVal.getTs());
			try {
				currentVal = this.blockingQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void calcul(Timestamp currentTime) {
		Long idFirstTemp = (long) -1;
		int scoreFirstTemp = -1;
		
		Long idSecondTemp = (long) -1;
		int scoreSecondTemp = -1;
		
		Long idThirdTemp =  (long) -1;
		int scoreThirdTemp = -1;
		
		Iterator<Entry<Long, Post>> it = this.posts.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Long, Post> pair = (Map.Entry<Long, Post>) it.next();
			int scoreTemp = pair.getValue().getScore(currentTime);
			if (scoreTemp == 0) {
				it.remove();
			} else {
				if (scoreTemp > scoreFirstTemp || (scoreTemp == scoreFirstTemp && pair.getValue().after(this.posts.get(idFirstTemp)))) {
					idThirdTemp = idSecondTemp;
					scoreThirdTemp = scoreSecondTemp;
					idSecondTemp = idFirstTemp;
					scoreSecondTemp = scoreFirstTemp;
					idFirstTemp = pair.getValue().getId();
					scoreFirstTemp = scoreTemp;
				} else if(scoreTemp > scoreSecondTemp || (scoreTemp == scoreSecondTemp && pair.getValue().after(this.posts.get(idSecondTemp)))) {
					idThirdTemp = idSecondTemp;
					scoreThirdTemp = scoreSecondTemp;
					idSecondTemp = pair.getValue().getId();
					scoreSecondTemp = scoreTemp;
				} else if(scoreTemp > scoreThirdTemp || (scoreTemp == scoreThirdTemp && pair.getValue().after(this.posts.get(idThirdTemp)))) {
					idThirdTemp = pair.getValue().getId();
					scoreThirdTemp = scoreTemp;
				}
			}
		}
		
		changeTop(currentTime, idFirstTemp, scoreFirstTemp, idSecondTemp, scoreSecondTemp, idThirdTemp, scoreThirdTemp);
		
	}

	public void changeTop(Timestamp currentTime, Long idFirstTemp, int scoreFirstTemp, Long idSecondTemp, int scoreSecondTemp, Long idThirdTemp, int scoreThirdTemp) {
		
		if (idFirstTemp != this.idFirst || idSecondTemp != this.idSecond || idThirdTemp != this.idThird
				|| scoreFirstTemp != this.scoreFirst || scoreSecondTemp != this.scoreSecond || scoreThirdTemp != this.scoreThird) {
			
			this.idFirst = idFirstTemp;
			this.idSecond = idSecondTemp;
			this.idThird = idThirdTemp;
			
			this.scoreFirst = scoreFirstTemp;
			this.scoreSecond = scoreSecondTemp;
			this.scoreThird = scoreThirdTemp;
			
			if (this.display) {
				System.out.println(
						currentTime.toString() + " | " +
						(this.idFirst == -1 ? "-, -, -, -" : this.posts.get(this.idFirst).display(this.scoreFirst)) + " | " +
						(this.idSecond == -1 ? "-, -, -, -" : this.posts.get(this.idSecond).display(this.scoreSecond)) + " | " +
						(this.idThird == -1 ? "-, -, -, -" : this.posts.get(this.idThird).display(this.scoreThird))
						);
			} else {
				this.result += currentTime.toString() + " | " +
						(this.idFirst == -1 ? "-, -, -, -" : this.posts.get(this.idFirst).display(this.scoreFirst)) + " | " +
						(this.idSecond == -1 ? "-, -, -, -" : this.posts.get(this.idSecond).display(this.scoreSecond)) + " | " +
						(this.idThird == -1 ? "-, -, -, -" : this.posts.get(this.idThird).display(this.scoreThird)) + "\n";
			}
		}
	}
	
	public String getResult() {
		return result;
	}
}

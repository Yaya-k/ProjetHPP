package traitement;

import java.util.concurrent.BlockingQueue;

import entities.PostCom;

public class QueuesSorter implements Runnable {

	private BlockingQueue<PostCom> blockingQueueP;
	private BlockingQueue<PostCom> blockingQueueC;
	private BlockingQueue<PostCom> blockingQueue;
	private PostCom POISON_PILL = new PostCom(null, (long) -1);

	public QueuesSorter(BlockingQueue<PostCom> blockingQueueP, BlockingQueue<PostCom> blockingQueueC,
			BlockingQueue<PostCom> blockingQueue) {
		super();
		this.blockingQueueP = blockingQueueP;
		this.blockingQueueC = blockingQueueC;
		this.blockingQueue = blockingQueue;
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
						currentPost = this.blockingQueueP.take();
					}
					if (currentComment == null) {
						currentComment = this.blockingQueueC.take();
					}
					if (currentPost.isPoison() || currentComment.isPoison()) {
						if (currentPost.isPoison()) {
							bPost = false;
						}
						if (currentComment.isPoison()) {
							bComment = false;
						}
					} else {
						if (currentPost.after(currentComment)) {
							this.blockingQueue.put(currentComment);
							currentComment = null;
						} else {
							this.blockingQueue.put(currentPost);
							currentPost = null;
						}
					}
				} else if (bPost && !bComment) {
					if (currentPost == null) {
						currentPost = this.blockingQueueP.take();
					}
					if (currentPost.isPoison()) {
						bPost = false;
					} else {
						this.blockingQueue.put(currentPost);
						currentPost = null;
					}
				} else {
					if (currentComment == null) {
						currentComment = this.blockingQueueC.take();
					}
					if (currentComment.isPoison()) {
						bComment = false;
					} else {
						this.blockingQueue.put(currentComment);
						currentComment = null;
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			this.blockingQueue.put(POISON_PILL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

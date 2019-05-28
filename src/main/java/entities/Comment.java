package entities;

import java.sql.Timestamp;

public class Comment extends PostCom {
	
	private Long idParent;
	private boolean parentPost;
	
	public Comment(Timestamp ts, Long id, Long idParent, boolean parentPost) {
		super(ts, id);
		this.idParent = idParent;
		this.parentPost = parentPost;
	}

	public Long getIdParent() {
		return idParent;
	}

	public void setIdParent(Long idParent) {
		this.idParent = idParent;
	}

	public boolean isParentPost() {
		return parentPost;
	}

	public void setParentPost(boolean parentPost) {
		this.parentPost = parentPost;
	}

	@Override
	public String toString() {
		return "\nC: " + ts + " | " + id;
	}

}

package entities;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

public class Post extends PostCom {
	
	private List<Timestamp> lTs;
	private String user;
	
	public Post(Timestamp ts, Long id, String user) {
		super(ts, id);
		this.lTs = new LinkedList<Timestamp>();
		this.user = user;
	}

	public List<Timestamp> getlTs() {
		return lTs;
	}

	public void setlTs(List<Timestamp> lTs) {
		this.lTs = lTs;
	}

	public int getScore(Timestamp currentTime) {
		long nbJTemp = (currentTime.getTime() - this.ts.getTime()) / (86400000);
		int scoreTemp = 0;
		int score = Math.max(0, 10 - (int) nbJTemp);
		for (Timestamp timestamp : this.lTs) {
			nbJTemp = (currentTime.getTime() - timestamp.getTime()) / (86400000);
			scoreTemp = Math.max(0, 10 - (int) nbJTemp);
			score += scoreTemp;
		}
		return score;
	}
	
	@Override
	public String toString() {
		return "\nP: " + ts + " | " + id;
	}
	
	public String display(int score) {
		return this.id + ", " + this.user + ", " + score + ", " + this.lTs.size();
	}
	
	public void add(Comment com) {
		this.lTs.add(com.getTs());
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}
	
	public boolean after(Post p) {
		if (this.ts.equals(p.getTs())) {
			if (p.getlTs().isEmpty()) {
				return true;
			} else if (this.lTs.isEmpty()) {
				return false;
			}else {
				return (this.lTs.get(this.lTs.size() - 1)).after(p.getlTs().get(p.getlTs().size() - 1));
			}
		} else {
			return this.ts.after(p.getTs());
		}
	}
	
}

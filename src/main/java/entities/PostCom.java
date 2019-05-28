package entities;

import java.sql.Timestamp;

public class PostCom {
	
	protected Timestamp ts;
	protected Long id;
	
	public PostCom(Timestamp ts, Long id) {
		super();
		this.ts = ts;
		this.id = id;
	}

	public Timestamp getTs() {
		return ts;
	}

	public void setTs(Timestamp ts) {
		this.ts = ts;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "\n" + ts + " | " + id;
	}
	
	public boolean after(PostCom o) {
		return this.ts.after(o.getTs());
	}
	
	public boolean isPoison() {
		return this.ts == null;
	}
}

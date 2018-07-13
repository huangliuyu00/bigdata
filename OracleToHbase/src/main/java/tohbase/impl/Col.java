package tohbase.impl;

public class Col {
	private String id;
	private String ypbm;
	private String scph;
	private String corpid;
	private String sl;
	private String dw;
	private String ghdw;
	private String corpid_zw;
	private String ghdw_zw;

	public Col() {
		super();
	}

	public Col(String id, String ypbm, String scph, String corpid, String sl, String dw, String ghdw, String corpid_zw,
			String ghdw_zw) {
		super();
		this.id = id;
		this.ypbm = ypbm;
		this.scph = scph;
		this.corpid = corpid;
		this.sl = sl;
		this.dw = dw;
		this.ghdw = ghdw;
		this.corpid_zw = corpid_zw;
		this.ghdw_zw = ghdw_zw;
	}

	public String getYpbm() {
		return ypbm;
	}

	public void setYpbm(String ypbm) {
		this.ypbm = ypbm;
	}

	public String getScph() {
		return scph;
	}

	public void setScph(String scph) {
		this.scph = scph;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCorpid() {
		return corpid;
	}

	public void setCorpid(String corpid) {
		this.corpid = corpid;
	}

	public String getSl() {
		return sl;
	}

	public void setSl(String sl) {
		this.sl = sl;
	}

	public String getDw() {
		return dw;
	}

	public void setDw(String dw) {
		this.dw = dw;
	}

	public String getGhdw() {
		return ghdw;
	}

	public void setGhdw(String ghdw) {
		this.ghdw = ghdw;
	}

	public String getCorpid_zw() {
		return corpid_zw;
	}

	public void setCorpid_zw(String corpid_zw) {
		this.corpid_zw = corpid_zw;
	}

	public String getGhdw_zw() {
		return ghdw_zw;
	}

	public void setGhdw_zw(String ghdw_zw) {
		this.ghdw_zw = ghdw_zw;
	}

}

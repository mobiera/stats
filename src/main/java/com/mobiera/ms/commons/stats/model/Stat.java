package com.mobiera.ms.commons.stats.model;

import java.io.Serializable;
import java.time.Instant;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;

import com.mobiera.ms.commons.stats.api.StatGranularity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;


/**
 * The persistent class for the cp_spool database table.
 * 
 */
@Entity
@Table(name="stat")
@DynamicUpdate
@DynamicInsert


@NamedQueries({
	@NamedQuery(name="Stat.get", query="FROM Stat s where s.statClass=:statClass AND s.entityFk=:entityFk AND s.statGranularity=:statGranularity and s.ts=:ts"),
	@NamedQuery(name="Stat.listOne", query="FROM Stat s where s.statClass=:statClass AND s.entityFk=:entityFk AND s.statGranularity=:statGranularity AND s.ts>=:from AND s.ts<:to ORDER by s.ts ASC"),
	@NamedQuery(name="Stat.listVarious", query="FROM Stat s where s.statClass=:statClass AND s.entityFk IN :entityFks AND s.statGranularity=:statGranularity AND s.ts>=:from AND s.ts<:to"),
	@NamedQuery(name="Stat.listAll", query="FROM Stat s where s.statClass=:statClass AND s.statGranularity=:statGranularity AND s.ts>=:from AND s.ts<:to"),
	
})

public class Stat implements Serializable {
	private static final long serialVersionUID = 1L;
	
	
	@Id
	private String id;
	
	
	/*@Id
	@SequenceGenerator(name="STAT_ID_GENERATOR", sequenceName="HIBERNATE_SEQ", allocationSize=1)
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="STAT_ID_GENERATOR")
	private Long id;*/

	private Instant ts;
	
	@Column(name="entity_fk")
	private Long entityFk;
	
	@Column(name="statclass")
	private String statClass;
	
	@Column(name="stat_granularity")
	private StatGranularity statGranularity;
	
	@Transient
	private String lockObjId = null;
	
	public String getLockObjId() {
		if (lockObjId == null) {
			lockObjId = "s" + 
		statClass + "-" 
					+ entityFk 
					+ "-" + statGranularity.getIndex() 
					+ "-" + ts.getEpochSecond();
		}
		return lockObjId;
	}
	
	private Long long0;
	private Long long1;
	private Long long2;
	private Long long3;
	private Long long4;
	private Long long5;
	private Long long6;
	private Long long7;
	private Long long8;
	private Long long9;
	private Long long10;
	private Long long11;
	private Long long12;
	private Long long13;
	private Long long14;
	private Long long15;
	private Long long16;
	private Long long17;
	private Long long18;
	private Long long19;
	private Long long20;
	private Long long21;
	private Long long22;
	private Long long23;
	private Long long24;
	private Long long25;
	private Long long26;
	private Long long27;
	private Long long28;
	private Long long29;
	private Long long30;
	private Long long31;

	private Long long32;
	private Long long33;
	private Long long34;
	private Long long35;
	private Long long36;
	private Long long37;
	private Long long38;
	private Long long39;
	private Long long40;
	private Long long41;
	private Long long42;
	private Long long43;
	private Long long44;
	private Long long45;
	private Long long46;
	private Long long47;
	private Long long48;
	
	private Long long49;
	private Long long50;
	private Long long51;
	private Long long52;
	private Long long53;
	private Long long54;
	private Long long55;
	private Long long56;
	private Long long57;
	private Long long58;
	private Long long59;
	private Long long60;
	private Long long61;
	private Long long62;
	private Long long63;
	private Long long64;
	private Long long65;
	private Long long66;
	private Long long67;
	private Long long68;
	private Long long69;
	private Long long70;
	private Long long71;
	private Long long72;
	private Long long73;
	private Long long74;
	private Long long75;
	
	private Long long76;
	private Long long77;
	private Long long78;
	private Long long79;
	private Long long80;
	private Long long81;
	private Long long82;
	private Long long83;
	private Long long84;
	private Long long85;
	private Long long86;
	private Long long87;
	private Long long88;
	private Long long89;
	private Long long90;
	private Long long91;
	private Long long92;
	private Long long93;
	private Long long94;
	private Long long95;
	private Long long96;
	private Long long97;
	private Long long98;
	private Long long99;
	
	
	
	private Double double128;
	private Double double129;
	private Double double130;
	private Double double131;
	private Double double132;
	private Double double133;
	private Double double134;
	private Double double135;
	private Double double136;
	
	
	private Instant lastFlushed;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Instant getTs() {
		return ts;
	}
	public void setTs(Instant ts) {
		this.ts = ts;
	}
	public Long getEntityFk() {
		return entityFk;
	}
	public void setEntityFk(Long entityFk) {
		this.entityFk = entityFk;
	}
	
	public StatGranularity getStatGranularity() {
		return statGranularity;
	}
	public void setStatGranularity(StatGranularity statGranularity) {
		this.statGranularity = statGranularity;
	}
	public Long getLong0() {
		return long0;
	}
	public void setLong0(Long long0) {
		this.long0 = long0;
	}
	public Long getLong1() {
		return long1;
	}
	public void setLong1(Long long1) {
		this.long1 = long1;
	}
	public Long getLong2() {
		return long2;
	}
	public void setLong2(Long long2) {
		this.long2 = long2;
	}
	public Long getLong3() {
		return long3;
	}
	public void setLong3(Long long3) {
		this.long3 = long3;
	}
	public Long getLong4() {
		return long4;
	}
	public void setLong4(Long long4) {
		this.long4 = long4;
	}
	public Long getLong5() {
		return long5;
	}
	public void setLong5(Long long5) {
		this.long5 = long5;
	}
	public Long getLong6() {
		return long6;
	}
	public void setLong6(Long long6) {
		this.long6 = long6;
	}
	public Long getLong7() {
		return long7;
	}
	public void setLong7(Long long7) {
		this.long7 = long7;
	}
	public Long getLong8() {
		return long8;
	}
	public void setLong8(Long long8) {
		this.long8 = long8;
	}
	public Long getLong9() {
		return long9;
	}
	public void setLong9(Long long9) {
		this.long9 = long9;
	}
	public Long getLong10() {
		return long10;
	}
	public void setLong10(Long long10) {
		this.long10 = long10;
	}
	public Long getLong11() {
		return long11;
	}
	public void setLong11(Long long11) {
		this.long11 = long11;
	}
	public Long getLong12() {
		return long12;
	}
	public void setLong12(Long long12) {
		this.long12 = long12;
	}
	public Long getLong13() {
		return long13;
	}
	public void setLong13(Long long13) {
		this.long13 = long13;
	}
	public Long getLong14() {
		return long14;
	}
	public void setLong14(Long long14) {
		this.long14 = long14;
	}
	public Long getLong15() {
		return long15;
	}
	public void setLong15(Long long15) {
		this.long15 = long15;
	}
	public Long getLong16() {
		return long16;
	}
	public void setLong16(Long long16) {
		this.long16 = long16;
	}
	public Long getLong17() {
		return long17;
	}
	public void setLong17(Long long17) {
		this.long17 = long17;
	}
	public Long getLong18() {
		return long18;
	}
	public void setLong18(Long long18) {
		this.long18 = long18;
	}
	public Long getLong19() {
		return long19;
	}
	public void setLong19(Long long19) {
		this.long19 = long19;
	}
	public Long getLong20() {
		return long20;
	}
	public void setLong20(Long long20) {
		this.long20 = long20;
	}
	public Long getLong21() {
		return long21;
	}
	public void setLong21(Long long21) {
		this.long21 = long21;
	}
	public Long getLong22() {
		return long22;
	}
	public void setLong22(Long long22) {
		this.long22 = long22;
	}
	public Long getLong23() {
		return long23;
	}
	public void setLong23(Long long23) {
		this.long23 = long23;
	}
	public Long getLong24() {
		return long24;
	}
	public void setLong24(Long long24) {
		this.long24 = long24;
	}
	public Long getLong25() {
		return long25;
	}
	public void setLong25(Long long25) {
		this.long25 = long25;
	}
	public Long getLong26() {
		return long26;
	}
	public void setLong26(Long long26) {
		this.long26 = long26;
	}
	public Long getLong27() {
		return long27;
	}
	public void setLong27(Long long27) {
		this.long27 = long27;
	}
	public Long getLong28() {
		return long28;
	}
	public void setLong28(Long long28) {
		this.long28 = long28;
	}
	public Long getLong29() {
		return long29;
	}
	public void setLong29(Long long29) {
		this.long29 = long29;
	}
	public Long getLong30() {
		return long30;
	}
	public void setLong30(Long long30) {
		this.long30 = long30;
	}
	public Long getLong31() {
		return long31;
	}
	public void setLong31(Long long31) {
		this.long31 = long31;
	}
	public void setLockObjId(String lockObjId) {
		this.lockObjId = lockObjId;
	}
	public Instant getLastFlushed() {
		return lastFlushed;
	}
	public void setLastFlushed(Instant lastFlushed) {
		this.lastFlushed = lastFlushed;
	}
	public String getStatClass() {
		return statClass;
	}
	public void setStatClass(String statClass) {
		this.statClass = statClass;
	}
	public Long getLong32() {
		return long32;
	}
	public void setLong32(Long long32) {
		this.long32 = long32;
	}
	public Long getLong33() {
		return long33;
	}
	public void setLong33(Long long33) {
		this.long33 = long33;
	}
	public Long getLong34() {
		return long34;
	}
	public void setLong34(Long long34) {
		this.long34 = long34;
	}
	public Long getLong35() {
		return long35;
	}
	public void setLong35(Long long35) {
		this.long35 = long35;
	}
	public Long getLong36() {
		return long36;
	}
	public void setLong36(Long long36) {
		this.long36 = long36;
	}
	public Long getLong37() {
		return long37;
	}
	public void setLong37(Long long37) {
		this.long37 = long37;
	}
	public Long getLong38() {
		return long38;
	}
	public void setLong38(Long long38) {
		this.long38 = long38;
	}
	public Long getLong39() {
		return long39;
	}
	public void setLong39(Long long39) {
		this.long39 = long39;
	}
	public Long getLong40() {
		return long40;
	}
	public void setLong40(Long long40) {
		this.long40 = long40;
	}
	public Long getLong41() {
		return long41;
	}
	public void setLong41(Long long41) {
		this.long41 = long41;
	}
	public Long getLong42() {
		return long42;
	}
	public void setLong42(Long long42) {
		this.long42 = long42;
	}
	public Long getLong43() {
		return long43;
	}
	public void setLong43(Long long43) {
		this.long43 = long43;
	}
	public Long getLong44() {
		return long44;
	}
	public void setLong44(Long long44) {
		this.long44 = long44;
	}
	public Long getLong45() {
		return long45;
	}
	public void setLong45(Long long45) {
		this.long45 = long45;
	}
	public Long getLong46() {
		return long46;
	}
	public void setLong46(Long long46) {
		this.long46 = long46;
	}
	public Long getLong47() {
		return long47;
	}
	public void setLong47(Long long47) {
		this.long47 = long47;
	}
	public Long getLong48() {
		return long48;
	}
	public void setLong48(Long long48) {
		this.long48 = long48;
	}
	public Double getDouble128() {
		return double128;
	}
	public void setDouble128(Double double128) {
		this.double128 = double128;
	}
	public Double getDouble129() {
		return double129;
	}
	public void setDouble129(Double double129) {
		this.double129 = double129;
	}
	public Double getDouble130() {
		return double130;
	}
	public void setDouble130(Double double130) {
		this.double130 = double130;
	}
	public Double getDouble131() {
		return double131;
	}
	public void setDouble131(Double double131) {
		this.double131 = double131;
	}
	public Double getDouble132() {
		return double132;
	}
	public void setDouble132(Double double132) {
		this.double132 = double132;
	}
	public Double getDouble133() {
		return double133;
	}
	public void setDouble133(Double double133) {
		this.double133 = double133;
	}
	public Double getDouble134() {
		return double134;
	}
	public void setDouble134(Double double134) {
		this.double134 = double134;
	}
	public Double getDouble135() {
		return double135;
	}
	public void setDouble135(Double double135) {
		this.double135 = double135;
	}
	public Double getDouble136() {
		return double136;
	}
	public void setDouble136(Double double136) {
		this.double136 = double136;
	}
	public Long getLong49() {
		return long49;
	}
	public void setLong49(Long long49) {
		this.long49 = long49;
	}
	public Long getLong50() {
		return long50;
	}
	public void setLong50(Long long50) {
		this.long50 = long50;
	}
	public Long getLong51() {
		return long51;
	}
	public void setLong51(Long long51) {
		this.long51 = long51;
	}
	public Long getLong52() {
		return long52;
	}
	public void setLong52(Long long52) {
		this.long52 = long52;
	}
	public Long getLong53() {
		return long53;
	}
	public void setLong53(Long long53) {
		this.long53 = long53;
	}
	public Long getLong54() {
		return long54;
	}
	public void setLong54(Long long54) {
		this.long54 = long54;
	}
	public Long getLong55() {
		return long55;
	}
	public void setLong55(Long long55) {
		this.long55 = long55;
	}
	public Long getLong56() {
		return long56;
	}
	public void setLong56(Long long56) {
		this.long56 = long56;
	}
	public Long getLong57() {
		return long57;
	}
	public void setLong57(Long long57) {
		this.long57 = long57;
	}
	public Long getLong58() {
		return long58;
	}
	public void setLong58(Long long58) {
		this.long58 = long58;
	}
	public Long getLong59() {
		return long59;
	}
	public void setLong59(Long long59) {
		this.long59 = long59;
	}
	public Long getLong60() {
		return long60;
	}
	public void setLong60(Long long60) {
		this.long60 = long60;
	}
	public Long getLong61() {
		return long61;
	}
	public void setLong61(Long long61) {
		this.long61 = long61;
	}
	public Long getLong62() {
		return long62;
	}
	public void setLong62(Long long62) {
		this.long62 = long62;
	}
	public Long getLong63() {
		return long63;
	}
	public void setLong63(Long long63) {
		this.long63 = long63;
	}
	public Long getLong64() {
		return long64;
	}
	public void setLong64(Long long64) {
		this.long64 = long64;
	}
	public Long getLong65() {
		return long65;
	}
	public void setLong65(Long long65) {
		this.long65 = long65;
	}
	public Long getLong66() {
		return long66;
	}
	public void setLong66(Long long66) {
		this.long66 = long66;
	}
	public Long getLong67() {
		return long67;
	}
	public void setLong67(Long long67) {
		this.long67 = long67;
	}
	public Long getLong68() {
		return long68;
	}
	public void setLong68(Long long68) {
		this.long68 = long68;
	}
	public Long getLong69() {
		return long69;
	}
	public void setLong69(Long long69) {
		this.long69 = long69;
	}
	public Long getLong70() {
		return long70;
	}
	public void setLong70(Long long70) {
		this.long70 = long70;
	}
	public Long getLong71() {
		return long71;
	}
	public void setLong71(Long long71) {
		this.long71 = long71;
	}
	public Long getLong72() {
		return long72;
	}
	public void setLong72(Long long72) {
		this.long72 = long72;
	}
	public Long getLong73() {
		return long73;
	}
	public void setLong73(Long long73) {
		this.long73 = long73;
	}
	public Long getLong74() {
		return long74;
	}
	public void setLong74(Long long74) {
		this.long74 = long74;
	}
	public Long getLong75() {
		return long75;
	}
	public void setLong75(Long long75) {
		this.long75 = long75;
	}
	public Long getLong76() {
		return long76;
	}
	public void setLong76(Long long76) {
		this.long76 = long76;
	}
	public Long getLong77() {
		return long77;
	}
	public void setLong77(Long long77) {
		this.long77 = long77;
	}
	public Long getLong78() {
		return long78;
	}
	public void setLong78(Long long78) {
		this.long78 = long78;
	}
	public Long getLong79() {
		return long79;
	}
	public void setLong79(Long long79) {
		this.long79 = long79;
	}
	public Long getLong80() {
		return long80;
	}
	public void setLong80(Long long80) {
		this.long80 = long80;
	}
	public Long getLong81() {
		return long81;
	}
	public void setLong81(Long long81) {
		this.long81 = long81;
	}
	public Long getLong82() {
		return long82;
	}
	public void setLong82(Long long82) {
		this.long82 = long82;
	}
	public Long getLong83() {
		return long83;
	}
	public void setLong83(Long long83) {
		this.long83 = long83;
	}
	public Long getLong84() {
		return long84;
	}
	public void setLong84(Long long84) {
		this.long84 = long84;
	}
	public Long getLong85() {
		return long85;
	}
	public void setLong85(Long long85) {
		this.long85 = long85;
	}
	public Long getLong86() {
		return long86;
	}
	public void setLong86(Long long86) {
		this.long86 = long86;
	}
	public Long getLong87() {
		return long87;
	}
	public void setLong87(Long long87) {
		this.long87 = long87;
	}
	public Long getLong88() {
		return long88;
	}
	public void setLong88(Long long88) {
		this.long88 = long88;
	}
	public Long getLong89() {
		return long89;
	}
	public void setLong89(Long long89) {
		this.long89 = long89;
	}
	public Long getLong90() {
		return long90;
	}
	public void setLong90(Long long90) {
		this.long90 = long90;
	}
	public Long getLong91() {
		return long91;
	}
	public void setLong91(Long long91) {
		this.long91 = long91;
	}
	public Long getLong92() {
		return long92;
	}
	public void setLong92(Long long92) {
		this.long92 = long92;
	}
	public Long getLong93() {
		return long93;
	}
	public void setLong93(Long long93) {
		this.long93 = long93;
	}
	public Long getLong94() {
		return long94;
	}
	public void setLong94(Long long94) {
		this.long94 = long94;
	}
	public Long getLong95() {
		return long95;
	}
	public void setLong95(Long long95) {
		this.long95 = long95;
	}
	public Long getLong96() {
		return long96;
	}
	public void setLong96(Long long96) {
		this.long96 = long96;
	}
	public Long getLong97() {
		return long97;
	}
	public void setLong97(Long long97) {
		this.long97 = long97;
	}
	public Long getLong98() {
		return long98;
	}
	public void setLong98(Long long98) {
		this.long98 = long98;
	}
	public Long getLong99() {
		return long99;
	}
	public void setLong99(Long long99) {
		this.long99 = long99;
	}
	
	
	
}
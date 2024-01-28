package com.mobiera.ms.commons.stats.model;

import java.io.Serializable;
import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;

import com.mobiera.ms.commons.stats.api.StatGranularity;

import lombok.Getter;
import lombok.Setter;


/**
 * The persistent class for the cp_spool database table.
 * 
 */
@Entity
@Table(name="stat")
@DynamicUpdate
@DynamicInsert


@NamedQueries({
	@NamedQuery(name="Stat.get", query="FROM Stat s where s.statClass=:statClass AND s.entityId=:entityId AND s.statGranularity=:statGranularity and s.ts=:ts"),
	@NamedQuery(name="Stat.listOne", query="FROM Stat s where s.statClass=:statClass AND s.entityId=:entityId AND s.statGranularity=:statGranularity AND s.ts>=:from AND s.ts<:to ORDER by s.ts ASC"),
	@NamedQuery(name="Stat.listVarious", query="FROM Stat s where s.statClass=:statClass AND s.entityId IN :entityIds AND s.statGranularity=:statGranularity AND s.ts>=:from AND s.ts<:to"),
	@NamedQuery(name="Stat.listAll", query="FROM Stat s where s.statClass=:statClass AND s.statGranularity=:statGranularity AND s.ts>=:from AND s.ts<:to"),
	
})

@Getter
@Setter
public class Stat implements Serializable {
	private static final long serialVersionUID = 1L;
	
	
	@Id
	private String id;
	
	
	/*@Id
	@SequenceGenerator(name="STAT_ID_GENERATOR", sequenceName="HIBERNATE_SEQ", allocationSize=1)
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="STAT_ID_GENERATOR")
	private Long id;*/

	@Column(columnDefinition="timestamptz")
	private Instant ts;
	
	@Column(name="entity_fk")
	private Long entityFk;
	
	@Column(name="entity_id", columnDefinition="text")
	private String entityId;
	@Column(name="statclass", columnDefinition="text")
	private String statClass;
	
	@Column(name="stat_granularity")
	private StatGranularity statGranularity;
	
	@Transient
	private String lockObjId = null;
	
	public String getLockObjId() {
		if (lockObjId == null) {
			lockObjId = "s" + 
		statClass + "-" 
					+ entityId 
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
	
}
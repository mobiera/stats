package com.mobiera.ms.commons.stats.assembler;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.jboss.logging.Logger;

import com.mobiera.commons.vo.VOAssembler;
import com.mobiera.commons.vo.VOAssemblerException;
import com.mobiera.ms.commons.stats.api.StatVO;
import com.mobiera.ms.commons.stats.model.Stat;


@ApplicationScoped
public class StatVOAssembler extends VOAssembler<Stat, StatVO> {
	

	private static Logger logger = Logger.getLogger(StatVOAssembler.class);

	   
	  
	   @Override
	   public void assembleEntityToVO(Stat source, StatVO vo) throws VOAssemblerException{
			super.assembleEntityToVO(source, vo);
			
			
			
		}
	   
	   
	   
}
   
  
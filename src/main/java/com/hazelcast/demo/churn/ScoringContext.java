package com.hazelcast.demo.churn;

import java.io.Serializable;

import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.ModelEvaluator;

import com.rlab.entity.ContractInfo;
import com.rlab.entity.CustomerUsageDetails;
import com.rlab.jet.scoring.JPMMLUtils;
import com.rlab.jet.scoring.MLException;
import com.rlab.kafka.message.KMessage;

public class ScoringContext implements Serializable{

    private int count=0;
    
   
    ModelEvaluator<?> modelEvaluator;

    
    public ScoringContext( ) {
         
            initPMMLModel();
    }   
    
	private void initPMMLModel(){
		PMML pmml;
		try {
			pmml = JPMMLUtils.loadModel("churnPmmlModel.pmml");
			modelEvaluator = JPMMLUtils.getVerifiedEvaluator(pmml);
			Model model = modelEvaluator.getModel();   //leaving this here in case there is some side effect
		} catch (MLException | ReflectiveOperationException e) {
			e.printStackTrace();
		}
	}
   
	public  KMessage predictChurn(ContractInfo ci,CustomerUsageDetails cud)  {
		KMessage kmsg = new KMessage();
		kmsg.setId(count++);
		// 1 = Request 2 = Response
		kmsg.setMessageType(1);
		kmsg.setAttribute("VMail Message", cud.getvMailMessage() ); 
		kmsg.setAttribute("Day Mins", cud.getDayMins() ); 
		kmsg.setAttribute("Eve Mins", cud.getEveMins() ); 
		kmsg.setAttribute("Night Mins",cud.getNightMins() ); 
		kmsg.setAttribute("Intl Mins", cud.getIntlMins() ); 
		kmsg.setAttribute("CustServ Calls", cud.getCustomerSrvCalls() ); 
		kmsg.setAttribute("Day Calls", cud.getDayCalls() ); 
		kmsg.setAttribute("Day Charge", cud.getDayCharge() ); 
		kmsg.setAttribute("Eve Calls", cud.getEveCalls() ); 
		kmsg.setAttribute("Eve Charge", cud.getEveCharge() ); 
		kmsg.setAttribute("Night Calls", cud.getNightCalls() ); 
		kmsg.setAttribute("Night Charge", cud.getNightCharge() ); 
		kmsg.setAttribute("Intl Calls", cud.getIntlCalls() ); 
		kmsg.setAttribute("Intl Charge", cud.getIntlCharge() ); 
		kmsg.setAttribute("Area Code", cud.getAreaCode() ); 
		kmsg.setAttribute("Account Length", ci.getAccountLength() ); 
		kmsg.setAttribute("Int'l Plan",ci.getIntlPlan() ); 
		kmsg.setAttribute("VMail Plan", ci.getvMailPlan() ); 
		kmsg.setAttribute("State", ci.getState()); 
		kmsg.setAttribute("Phone", ci.getPhone() ); 
		KMessage ret =JPMMLUtils.evaluate(kmsg.getId(),modelEvaluator,kmsg.getAttributes());
		return ret;
	}


}

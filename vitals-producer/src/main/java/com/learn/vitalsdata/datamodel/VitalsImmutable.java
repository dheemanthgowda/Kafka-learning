package com.learn.vitalsdata.datamodel;

import java.util.List;
import java.util.Map;

public class VitalsImmutable {

    private long dd_ACQUIRED_DT;
    private double heart_RATE_MONITORED;

    public VitalsImmutable(String[] eachRowData) throws InterruptedException {
            for(int eachRow = 0; eachRow < eachRowData.length; eachRow++ ) {
                if(eachRow == 0 && eachRowData[eachRow] != null ) {
                    dd_ACQUIRED_DT = System.currentTimeMillis(); //Just to simulate real time data scenario
                } else if(eachRow == 1 && eachRowData[eachRow] != null) {
                    heart_RATE_MONITORED = Double.parseDouble(eachRowData[eachRow]);
                }
            }
    }

    @Override
    public String toString() {
        return "{"+"time" + dd_ACQUIRED_DT+ ","
                    + "hr"+  heart_RATE_MONITORED +  "}";
    }

}

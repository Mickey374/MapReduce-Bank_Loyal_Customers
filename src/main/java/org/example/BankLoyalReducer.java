package org.example;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BankLoyalReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void  reduce(Text key, Iterable<Text> values, Context c) throws IOException, java.lang.InterruptedException{

        String fName = "";
        String lName = "";
        String accNumber = "";
        int totalDeposit = 0;

        Iterator<Text> valuesIter = values.iterator();
        while(valuesIter.hasNext()){
            String val = valuesIter.next().toString();      //val
            String [] words = val.split(",");
            if (words[0].equals("P")){
                /*Customer is from PersonMapper*/
                fName = words[1];
                lName = words[2];
            } else if (words[0].equals("A")) {
                /*Customer is from AccountMpper*/
                accNumber = words[1];
                /*Check for Red Flag*/
                if(words[4].equalsIgnoreCase("yes")){
                    break;
                }

                /* Check for Quarter deposit */
                int withdrawalAmt = Integer.parseInt(words[3]);
                int depositAmt = Integer.parseInt(words[2]);

                if(withdrawalAmt > (depositAmt/2)) {
                    /*No need to process further*/
                    break;
                }
                totalDeposit += depositAmt;
            }
        }
        /*If the loyal conditions are met*/
        if(totalDeposit >= 10000){
            /* [id, fName, lName, accNum]*/
            c.write(key, new Text(fName + "," + lName+ "," + accNumber));
        }
    }
}

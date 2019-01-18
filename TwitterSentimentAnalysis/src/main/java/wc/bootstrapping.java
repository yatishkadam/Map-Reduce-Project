package wc;

import com.amazonaws.services.dynamodbv2.xspec.S;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class bootstrapping {
    public static void main(String[] args) {
        List<String> bootstrapList = new ArrayList<>(0);
        List<String> bootstrapFinalList = new ArrayList<>();
        String csvFile = "D:\\python\\preprocessing\\output.csv";
        String line = "";
        String cvsSplitBy = "\\|";
        int count = 0;
        int rand = 0;
        int nol = 326853;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null && count<nol) {
                bootstrapList.add(line);
                count++;
            }

            System.out.println(bootstrapList.size());
            for (String s: bootstrapList) {
                rand = 1 + (int)(Math.random() * ((10 - 1) + 1));
                if ( rand%4==0){
                    while (rand >0){
                        bootstrapFinalList.add(s);
                        rand=rand-2;
                    }
                }
                else if (rand%5 ==0){
                    bootstrapFinalList.add(s);
                }
            }

            System.out.println(bootstrapFinalList.size());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

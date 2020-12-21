package com.kungeek.zyb;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateDiff extends UDF {

    public int evaluate (String s1, String s2) throws ParseException {

//        2020-05-01   -- 2020-05-05   2020-10-01 --2020-10-08

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        Date t1 = formatter.parse("2020-05-01");
        Date t2 = formatter.parse("2020-05-05");
        Date t3 = formatter.parse("2020-10-01");
        Date t4 = formatter.parse("2020-10-08");

        Date date01 = formatter.parse(s1);
        Date date02 = formatter.parse(s2);

        long time01 = date01.getTime();
        long time02 = date02.getTime();

        long during = 0;

        if(time02 < t1.getTime()) {
            during = time02 - time01;
            return (int)(during / (1000 * 60 * 60 * 24) + 1);
        }else if(time02 >= t1.getTime() && time02 < t2.getTime()) {
            during = t1.getTime() - time01;
            return (int)(during / (1000 * 60 * 60 * 24));
        }else if(time02 >= t2.getTime() && time02 < t3.getTime()) {
            during = time02 - time01;
            if (time01 < t1.getTime()){
                return (int)(during / (1000 * 60 * 60 * 24)) - 5 + 1;
            }else {
                return (int)(during / (1000 * 60 * 60 * 24)) + 1;
            }
        }else if(time02 >= t3.getTime() && time02 < t4.getTime()) {
            during = t3.getTime() - time01;
            return (int)(during / (1000 * 60 * 60 * 24)) - 5;
        }else {
            during = time02 - time01;
            if (time01 < t1.getTime()){
                return (int)(during / (1000 * 60 * 60 * 24)) - 13 + 1;
            }else if(time01 < t3.getTime()){
                return (int)(during / (1000 * 60 * 60 * 24)) - 10 + 1;
            }else {
                return (int)(during / (1000 * 60 * 60 * 24)) + 1;
            }
        }

    }

    @Test
    public void test01() throws ParseException {

        System.out.println(new DateDiff().evaluate("2020-09-01", "2020-10-01"));
    }


}

package com.kungeek.zyb;

import com.aliyun.odps.udf.UDF;
import org.junit.jupiter.api.Test;

public class TestAliyun extends UDF {

    public int evaluate (String s1, String s2){
        return s1.length() - s2.length();
    }


    @Test
    public void test01(){
        System.out.println(new TestAliyun().evaluate("safsafs", "fasfaf"));
    }
}

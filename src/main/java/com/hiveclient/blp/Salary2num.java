package com.hiveclient.blp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Salary2num {
    public static int salary2Num(String salary)
    {
        if (salary.isEmpty())
            return 0;
        int res=0;
        String exception="\\d*";
        Pattern pattern = Pattern.compile(exception);
        Matcher matcher = pattern.matcher(salary);
        boolean b = matcher.find();
        System.out.println(salary);
        if (b)
        {
            String group = matcher.group(0);
            System.out.println(group);
            if (group.isEmpty())
                return 0;
            res=Integer.valueOf(group);
        }
        return res;
    }

}

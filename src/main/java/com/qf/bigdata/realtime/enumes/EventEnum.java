package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
//事件枚举
public enum EventEnum {

    VIEW("01", "view","浏览"),
    CLICK("02", "click","点击"),
    INPUT("03", "input","输入"),
    SLIDE("04", "slide","滑动");


    private String code;
    private String desc;
    private String remark;

    private EventEnum(String code, String remark, String desc) {
        this.code = code;
        this.remark = remark;
        this.desc = desc;
    }

    public static List<String> getEvents(){
        List<String> events = Arrays.asList(
                CLICK.code,
                INPUT.code,
                SLIDE.code,
                VIEW.code
        );
        return events;
    }

    public static List<String> getInterActiveEvents(){
        List<String> events = Arrays.asList(
                CLICK.code,
                VIEW.code,
                SLIDE.code
        );
        return events;
    }

    public static List<String> getViewListEvents(){
        List<String> events = Arrays.asList(
                VIEW.code,
                SLIDE.code
        );
        return events;
    }


    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public String getRemark() {
        return remark;
    }
}

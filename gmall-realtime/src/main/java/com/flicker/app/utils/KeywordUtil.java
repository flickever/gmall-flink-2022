package com.flicker.app.utils;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> splitKeyword(String keyword){
        JiebaSegmenter segmenter = new JiebaSegmenter();
        return segmenter.sentenceProcess(keyword);
    }

    public static void main(String[] args) {
        List<String> s = splitKeyword("尚硅谷大数据项目之实时数仓");
        System.out.println(s);
    }
}

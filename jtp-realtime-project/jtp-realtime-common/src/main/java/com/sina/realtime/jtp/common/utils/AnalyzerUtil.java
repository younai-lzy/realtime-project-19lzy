package com.sina.realtime.jtp.common.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * 分词器工具类
 * @author xuanyu
 */
public class AnalyzerUtil {

  /**
   * 使用JiebaAnalyzer分词器对中文文本进行普通分词
   */
//  public static List<String> jiebaAnalyzer(String content) {
//    // JieBa工具类对象
//    JiebaSegmenter js = new JiebaSegmenter();
//    // 分词
//    List<String> list = js.sentenceProcess(content.replaceAll("[\\pP‘’“”\\s+]", ""));
//    // 返回
//    return list;
//  }

  /**
   * 使用IKAnalyzer分词器对中文文本进行普通分词
   */
  public static List<String> ikAnalyzer(String content) throws Exception {
    ArrayList<String> list = new ArrayList<>();
    // 分词对象
    IKSegmenter ikSegmenter = new IKSegmenter(
      new StringReader(content), true
    );
    // 遍历
    Lexeme lexeme;
    while ((lexeme = ikSegmenter.next()) != null) {
      String text = lexeme.getLexemeText();
      list.add(text);
    }
    // 返回
    return list;
  }

  public static void main(String[] args) throws Exception {
    List<String> list = ikAnalyzer("欢迎您来南京长江大桥");
    System.out.println(Arrays.toString(list.toArray()));
  }
}

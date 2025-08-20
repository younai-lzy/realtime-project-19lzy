package com.sina.realtime.jtp.dws.log.function;

import com.sina.realtime.jtp.common.utils.AnalyzerUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author 30777
 */
@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class IkAnalyzerFunction extends TableFunction<Row> {
  public void eval(String str) throws Exception {
    //中文分词
    List<String> list = AnalyzerUtil.ikAnalyzer(str);
    //遍历集合
    for (String word : list) {
      collect(Row.of(word));
    }
  }

}

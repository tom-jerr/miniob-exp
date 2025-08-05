/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created for ClickBench CSV loading test
//

#include "gtest/gtest.h"
#include "sql/executor/load_data_executor.h"
#include <fstream>
#include <chrono>

/**
 * 测试Fast-CSV解析功能，特别针对ClickBench数据集格式
 */
class LoadDataExecutorTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // 创建测试CSV文件
    create_test_csv_files();
  }

  void TearDown() override
  {
    // 清理测试文件
    std::remove("test_clickbench.tsv");
    std::remove("test_standard.csv");
    std::remove("test_quoted.csv");
  }

  void create_test_csv_files()
  {
    // 创建ClickBench风格的TSV文件（Tab分隔）
    std::ofstream tsv_file("test_clickbench.tsv");
    tsv_file << "1\tJohn Doe\t25\t75000.50\n";
    tsv_file << "2\tJane Smith\t30\t85000.00\n";
    tsv_file << "3\tBob Johnson\t35\t95000.25\n";
    tsv_file.close();

    // 创建标准CSV文件（逗号分隔）
    std::ofstream csv_file("test_standard.csv");
    csv_file << "1,Alice Brown,28,70000.00\n";
    csv_file << "2,Charlie Davis,32,80000.50\n";
    csv_file.close();

    // 创建带引号的CSV文件
    std::ofstream quoted_file("test_quoted.csv");
    quoted_file << "1,\"Smith, John\",\"Software Engineer\",\"80000.00\"\n";
    quoted_file << "2,\"Brown, Alice\",\"Data Scientist\",\"90000.00\"\n";
    quoted_file.close();
  }
};

// 测试解析CSV行函数
TEST_F(LoadDataExecutorTest, ParseCSVLineBasic)
{
  std::vector<std::string> file_values;

  // 测试Tab分隔符（ClickBench格式）
  char line1[] = "1\tJohn Doe\t25\t75000.50";
  parse_csv_line(line1, file_values, '\t', '\0');
  EXPECT_EQ(file_values.size(), 4);
  EXPECT_EQ(file_values[0], "1");
  EXPECT_EQ(file_values[1], "John Doe");
  EXPECT_EQ(file_values[2], "25");
  EXPECT_EQ(file_values[3], "75000.50");

  // 测试逗号分隔符
  file_values.clear();
  char line2[] = "1,Alice Brown,28,70000.00";
  parse_csv_line(line2, file_values, ',', '\0');
  EXPECT_EQ(file_values.size(), 4);
  EXPECT_EQ(file_values[0], "1");
  EXPECT_EQ(file_values[1], "Alice Brown");
  EXPECT_EQ(file_values[2], "28");
  EXPECT_EQ(file_values[3], "70000.00");
}

TEST_F(LoadDataExecutorTest, ParseCSVLineWithQuotes)
{
  std::vector<std::string> file_values;

  // 测试带引号的字段
  char line[] = "1,\"Smith, John\",\"Software Engineer\",\"80000.00\"";
  parse_csv_line(line, file_values, ',', '"');
  EXPECT_EQ(file_values.size(), 4);
  EXPECT_EQ(file_values[0], "1");
  EXPECT_EQ(file_values[1], "Smith, John");
  EXPECT_EQ(file_values[2], "Software Engineer");
  EXPECT_EQ(file_values[3], "80000.00");
}

TEST_F(LoadDataExecutorTest, ParseCSVLineWithEscapedQuotes)
{
  std::vector<std::string> file_values;

  // 测试转义引号
  char line[] = "1,\"He said \"\"Hello\"\"\",\"Description\"";
  parse_csv_line(line, file_values, ',', '"');
  EXPECT_EQ(file_values.size(), 3);
  EXPECT_EQ(file_values[0], "1");
  EXPECT_EQ(file_values[1], "He said \"Hello\"");
  EXPECT_EQ(file_values[2], "Description");
}

TEST_F(LoadDataExecutorTest, ParseEmptyLine)
{
  std::vector<std::string> file_values;

  // 测试空行
  char line1[] = "";
  parse_csv_line(line1, file_values, ',', '\0');
  EXPECT_EQ(file_values.size(), 0);

  // 测试只有分隔符的行
  file_values.clear();
  char line2[] = ",,,";
  parse_csv_line(line2, file_values, ',', '\0');
  EXPECT_EQ(file_values.size(), 4);
  for (const auto &value : file_values) {
    EXPECT_EQ(value, "");
  }
}

TEST_F(LoadDataExecutorTest, ClickBenchFormatCompatibility)
{
  std::vector<std::string> file_values;

  // 模拟ClickBench hits表的一行数据
  char clickbench_line[] =
      "1\t2013-07-01\t2013-07-01 00:00:09\t5748266834505293144\t1201631375395418967\t0\t1\t0\t1\tN";
  parse_csv_line(clickbench_line, file_values, '\t', '\0');

  EXPECT_EQ(file_values.size(), 10);
  EXPECT_EQ(file_values[0], "1");
  EXPECT_EQ(file_values[1], "2013-07-01");
  EXPECT_EQ(file_values[2], "2013-07-01 00:00:09");
  EXPECT_EQ(file_values[9], "N");
}

TEST_F(LoadDataExecutorTest, LargeFieldHandling)
{
  std::vector<std::string> file_values;

  // 测试较大的字段
  std::string large_field(1000, 'A');
  std::string line      = "1\t" + large_field + "\t3";
  char       *line_copy = new char[line.length() + 1];
  strcpy(line_copy, line.c_str());

  parse_csv_line(line_copy, file_values, '\t', '\0');

  EXPECT_EQ(file_values.size(), 3);
  EXPECT_EQ(file_values[0], "1");
  EXPECT_EQ(file_values[1], large_field);
  EXPECT_EQ(file_values[2], "3");

  delete[] line_copy;
}

// 性能测试
TEST_F(LoadDataExecutorTest, PerformanceTest)
{
  std::vector<std::string> file_values;

  // 创建一个包含多个字段的测试行
  std::string test_line = "1\tfield2\tfield3\tfield4\tfield5\tfield6\tfield7\tfield8\tfield9\tfield10";
  char       *line_copy = new char[test_line.length() + 1];
  strcpy(line_copy, test_line.c_str());

  auto start = std::chrono::high_resolution_clock::now();

  // 解析1000次
  for (int i = 0; i < 1000; ++i) {
    file_values.clear();
    strcpy(line_copy, test_line.c_str());  // 重置行，因为parse_csv_line会修改它
    parse_csv_line(line_copy, file_values, '\t', '\0');
  }

  auto end      = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  // 验证结果
  EXPECT_EQ(file_values.size(), 10);

  // 打印性能信息
  std::cout << "Parsed 1000 lines in " << duration.count() << " microseconds" << std::endl;
  std::cout << "Average time per line: " << duration.count() / 1000.0 << " microseconds" << std::endl;

  delete[] line_copy;
}

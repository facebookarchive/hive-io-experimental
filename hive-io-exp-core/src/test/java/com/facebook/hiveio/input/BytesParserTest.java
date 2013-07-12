/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.hiveio.input;

import org.testng.annotations.Test;

import com.facebook.hiveio.common.HiveIOTestBase;
import com.facebook.hiveio.input.parser.array.BytesParser;
import com.google.common.base.Charsets;

import static com.facebook.hiveio.input.parser.array.BytesParser.parseDouble;
import static com.facebook.hiveio.input.parser.array.BytesParser.parseLong;
import static com.facebook.hiveio.input.parser.array.BytesParser.parseShort;
import static com.facebook.hiveio.input.parser.array.BytesParser.parseString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class BytesParserTest extends HiveIOTestBase {
  @Test
  public void testParseString() {
    String str = "abcdefghijklmn";
    byte[] bytes = str.getBytes(Charsets.UTF_8);
    assertEquals(parseString(bytes, 0, str.length()), str);
    assertEquals(parseString(bytes, 2, 3), "cde");
    assertEquals(parseString(bytes, 6, 2), "gh");
  }

  @Test
  public void testParseBoolean() {
    assertTrue(parseBoolean("true"));
    assertTrue(parseBoolean("TRUE"));
    assertTrue(parseBoolean("tRuE"));

    assertFalse(parseBoolean("false"));
    assertFalse(parseBoolean("FALSE"));
    assertFalse(parseBoolean("fAlSe"));

    assertNull(parseBoolean("true "));
    assertNull(parseBoolean(" true"));
    assertNull(parseBoolean("false "));
    assertNull(parseBoolean(" false"));
    assertNull(parseBoolean("t"));
    assertNull(parseBoolean("f"));
    assertNull(parseBoolean(""));
    assertNull(parseBoolean("blah"));
  }

  private Boolean parseBoolean(String s) {
    return BytesParser
        .parseBoolean(s.getBytes(Charsets.US_ASCII), 0, s.length());
  }

  @Test
  public void testShort() throws Exception {
    assertParseShort("1", 1);
    assertParseShort("12", 12);
    assertParseShort("123", 123);
    assertParseShort("-1", -1);
    assertParseShort("-12", -12);
    assertParseShort("-123", -123);
    assertParseShort("+1", 1);
    assertParseShort("+12", 12);
    assertParseShort("+123", 123);
    assertParseShort("0", 0);
    assertParseShort("-0", 0);
    assertParseShort("+0", 0);
    assertParseShort(Short.toString(Short.MAX_VALUE), Short.MAX_VALUE);
    assertParseShort(Short.toString(Short.MIN_VALUE), Short.MIN_VALUE);
  }

  private void assertParseShort(String string, int expectedValue) {
    assertEquals(parseShort(string.getBytes(Charsets.US_ASCII), 0,
        string.length()), expectedValue);

    // verify we can parse using a non-zero offset
    String padding = "9999";
    String padded = padding + string + padding;
    assertEquals(parseShort(padded.getBytes(Charsets.US_ASCII),
        padding.length(), string.length()), (short) expectedValue);
  }

  @Test
  public void testLong() throws Exception {
    assertParseLong("1", 1);
    assertParseLong("12", 12);
    assertParseLong("123", 123);
    assertParseLong("-1", -1);
    assertParseLong("-12", -12);
    assertParseLong("-123", -123);
    assertParseLong("+1", 1);
    assertParseLong("+12", 12);
    assertParseLong("+123", 123);
    assertParseLong("0", 0);
    assertParseLong("-0", 0);
    assertParseLong("+0", 0);
    assertParseLong(Long.toString(Long.MAX_VALUE), Long.MAX_VALUE);
    assertParseLong(Long.toString(Long.MIN_VALUE), Long.MIN_VALUE);
  }

  private void assertParseLong(String string, long expectedValue) {
    assertEquals(parseLong(string.getBytes(Charsets.US_ASCII), 0, string.length()),
        expectedValue);

    // verify we can parse using a non-zero offset
    String padding = "9999";
    String padded = padding + string + padding;
    assertEquals(parseLong(padded.getBytes(Charsets.US_ASCII), padding.length(),
        string.length()), expectedValue);
  }

  @Test
  public void testDouble() throws Exception {
    assertParseDouble("123");
    assertParseDouble("123.0");
    assertParseDouble("123.456");
    assertParseDouble("123.456e5");
    assertParseDouble("123.456e-5");
    assertParseDouble("123e5");
    assertParseDouble("123e-5");
    assertParseDouble("0");
    assertParseDouble("0.0");
    assertParseDouble("0.456");
    assertParseDouble("-0");
    assertParseDouble("-0.0");
    assertParseDouble("-0.456");
    assertParseDouble("-123");
    assertParseDouble("-123.0");
    assertParseDouble("-123.456");
    assertParseDouble("-123.456e-5");
    assertParseDouble("-123e5");
    assertParseDouble("-123e-5");
    assertParseDouble("+123");
    assertParseDouble("+123.0");
    assertParseDouble("+123.456");
    assertParseDouble("+123.456e5");
    assertParseDouble("+123.456e-5");
    assertParseDouble("+123e5");
    assertParseDouble("+123e-5");
    assertParseDouble("+0");
    assertParseDouble("+0.0");
    assertParseDouble("+0.456");


    assertParseDouble("NaN");
    assertParseDouble("-Infinity");
    assertParseDouble("Infinity");
    assertParseDouble("+Infinity");

    assertParseDouble(Double.toString(Double.MAX_VALUE));
    assertParseDouble(Double.toString(-Double.MAX_VALUE));
    assertParseDouble(Double.toString(Double.MIN_VALUE));
    assertParseDouble(Double.toString(-Double.MIN_VALUE));
  }

  private void assertParseDouble(String string) {
    assertEquals(parseDouble(string.getBytes(Charsets.US_ASCII), 0, string.length()),
        Double.parseDouble(string));

    // verify we can parse using a non-zero offset
    String padding = "9999";
    String padded = padding + string + padding;
    assertEquals(parseDouble(padded.getBytes(Charsets.US_ASCII), padding.length(),
            string.length()), Double.parseDouble(string));
  }
}

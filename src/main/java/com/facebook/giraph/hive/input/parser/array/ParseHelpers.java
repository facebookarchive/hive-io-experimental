package com.facebook.giraph.hive.input.parser.array;

public class ParseHelpers {
  public static Boolean parseBoolean(byte[] bytes, int start, int length) {
    if ((length == 4) &&
        (toUpperCase(bytes[start + 0]) == 'T') &&
        (toUpperCase(bytes[start + 1]) == 'R') &&
        (toUpperCase(bytes[start + 2]) == 'U') &&
        (toUpperCase(bytes[start + 3]) == 'E')) {
      return true;
    }
    if ((length == 5) &&
        (toUpperCase(bytes[start + 0]) == 'F') &&
        (toUpperCase(bytes[start + 1]) == 'A') &&
        (toUpperCase(bytes[start + 2]) == 'L') &&
        (toUpperCase(bytes[start + 3]) == 'S') &&
        (toUpperCase(bytes[start + 4]) == 'E')) {
      return false;
    }
    return null;
  }

  private static byte toUpperCase(byte b) {
    return isLowerCase(b) ? ((byte) (b - 32)) : b;
  }

  private static boolean isLowerCase(byte b) {
    return (b >= 'a') && (b <= 'z');
  }

  public static long parseLong(byte[] bytes, int start, int length) {
    int limit = start + length;

    int sign = bytes[start] == '-' ? -1 : 1;

    if (sign == -1 || bytes[start] == '+') {
      start++;
    }

    long value = bytes[start++] - '0';
    while (start < limit) {
      value = value * 10 + (bytes[start] - '0');
      start++;
    }

    return value * sign;
  }

  public static double parseDouble(byte[] bytes, int start, int length) {
    char[] chars = new char[length];
    for (int pos = 0; pos < length; pos++) {
      chars[pos] = (char) bytes[start + pos];
    }
    String string = new String(chars);
    return Double.parseDouble(string);
  }
}

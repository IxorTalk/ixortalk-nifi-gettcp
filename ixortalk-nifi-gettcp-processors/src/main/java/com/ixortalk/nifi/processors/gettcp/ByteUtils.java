package com.ixortalk.nifi.processors.gettcp;

public class ByteUtils {

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for(byte b: a)
            sb.append(byteToHex(b));
        return sb.toString();
    }

    public static String byteToHex(byte b) {
        return String.format(" [%02x]", b & 0xff);
    }
}

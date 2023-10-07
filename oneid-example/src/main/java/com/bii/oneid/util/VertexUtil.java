package com.bii.oneid.util;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.lang.ArrayUtils;

/**
 * @author bihaiyang
 * @desc
 * @since 2022/03/23
 */
public class VertexUtil {


    /**
     * @param id
     * @param priority
     * @return 生成一个固定长度为48位的string 类型的数字
     */
    public static BigInteger makeBigIntegerVertices(String id, int priority) throws NoSuchAlgorithmException {
        int p = 63;
        if (priority < 64 && priority >= 0) {
            p = priority + 10;
        }
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] encoded = md5.digest((id).getBytes());
        //    1. 确保是48位，所以 第一个byte范围是 18~ 81
        //    2. byte长度为，byte长度为20位，md5  16位，id 优先级 4位
        byte[] bytes = {(byte) (p + 18)};

        byte[] a = ArrayUtils.addAll(bytes, encoded);
        byte[] b = {(byte) 0, (byte) 0, (byte) 0};
        byte[] bt = ArrayUtils.addAll(a, b);
        return new BigInteger(bt);
    }


    /**
     * @param id
     * @param idType
     * @return 生成一个固定长度为48位的string 类型的数字
     */
    public static BigInteger makeBigIntegerVertices(String id, String idType) throws NoSuchAlgorithmException {
        byte[] seed = ByteBuffer.allocate(4).putInt(idType.hashCode()).array();
        byte[] bytes = {(byte) ((seed[0] + 128) % 108 + 18)};
        byte[] root = ArrayUtils.addAll(bytes, ArrayUtils.subarray(seed, seed.length - 3, seed.length));
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] encoded = md5.digest((id).getBytes());
        //    1. 确保是48位，所以 第一个byte范围是 18~ 81
        //    2. byte长度为，byte长度为20位，md5  16位，id 优先级 4位
        byte[] bt = ArrayUtils.addAll(root, encoded);
        return new BigInteger(bt);
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        //216980506331330196822619827736541137126758543450
        //38865206801101004283483501208454759018131546
        System.out.println(makeBigIntegerVertices("002", "ssn"));
        //168858220220470606366398524144109683685170610176
        System.out.println(makeBigIntegerVertices("002", 1));

    }
}

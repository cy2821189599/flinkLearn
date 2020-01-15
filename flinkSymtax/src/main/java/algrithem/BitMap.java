package algrithem;

import java.util.Scanner;

/**
 * @ClassName BitMap
 * @Description 位图法在40亿个不重复的unsigned int的整数，没排过序的，然后再给一个数，快速判断这个数是否在那40亿个数当中
 * @Author JackChen
 * @Date 2020/1/15 11:41
 * @Version 1.0
 **/
public class BitMap {
    public static void main(String[] args) {
        char[] bitMap = new char[1000000000];
        for ( long i = 1; i < 4000000000L; i++ ) {
            long index = i / 8;
            long tmp = i & 7;
            long val = 1 << tmp;
            bitMap[(int) index] = (char) ((int) bitMap[(int) index] | val);
        }
        System.out.println( "初始化bitmap完成" );
        Scanner scanner = new Scanner( System.in );
        boolean b = true;
        while ( b ) {
            System.out.print( "请输入你要检测的数:" );
            long num = scanner.nextLong();
            if ( num < 0 ) {
                b = false;
                System.out.println( "请输入0-4000000000以内的数" );
            } else if ( num > 4000000000L ) {
                System.out.println("不存在"+num);
            } else {
                int index = (int) (num / 8);
                char flag = '0';
                int tmp = (int) num & 7;
                String binaryString = Integer.toBinaryString( bitMap[index] );
                flag = binaryString.charAt( 7 - tmp );
                System.out.println( flag == '1' ? "存在" + num : "不存在" + num );
            }
        }
        scanner.close();
    }
}

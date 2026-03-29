package org.genom.reportservice.utils;

public class DocUtilsLite {
    public static String colNumToColChar(int colNum) {
        int offset_2 = 26;
        int offset_3 = (26 * 26) + offset_2;

        String zero_char = "";
        String first_char = "";

        if (colNum >= offset_3) {
            int mod_3_alp = (colNum - offset_3) % (26 * 26);

            int floor_3_alp = (int) Math.floor((colNum - offset_3) / (26 * 26));
            int floor_2_alp = (int) Math.floor(mod_3_alp / 26);

            zero_char = Character.toString((char) ('A' + floor_3_alp));
            first_char = Character.toString((char) ('A' + floor_2_alp));
        } else if (colNum >= offset_2) {
            int floor_2_alp = (int) Math.floor(colNum / 26);
            first_char = Character.toString((char) ('A' + floor_2_alp - 1));
        }


        int mod_2_alp = colNum % 26;

        String second_char = Character.toString((char) ('A' + mod_2_alp));

        return zero_char + first_char + second_char;
    }
}

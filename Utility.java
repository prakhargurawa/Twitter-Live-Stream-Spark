public class Utility {
    public static int countCharacters(String str){
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            if (Character.isLetter(str.charAt(i)))
                count++;
        }
        return count;
    }
}

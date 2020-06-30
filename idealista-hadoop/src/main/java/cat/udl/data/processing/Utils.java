package cat.udl.data.processing;

import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    /**
     * Generates a range list
     * @param start Start number of the range
     * @param end End range number
     * @return a list containing the desired range. Example: start = 1, end = 5 the result is List(1, 2, 3, 4)
     */
    public static List<Integer> range(int start, int end) {
        val result = new ArrayList<Integer>();

        var factor = 1;
        if (start > end)
            factor = -1;

        for(int i = start; i < end; i+=factor)
            result.add(i);

        return result;
    }
}

import java.io.*;
import java.util.*;

public class AreaCode {
    Map<String, List<MultiPolygon>> table = new HashMap<>();

    public AreaCode(BufferedReader br) {
        try {
            loadFromFile(br);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String get(double longitude, double latitude) {
        for (String code : table.keySet()) {
            for (MultiPolygon mp : table.get(code)) {
                if (mp.isPointIn(new Point(latitude, longitude))) {
                    return code;
                }
            }
        }
        return "x";
    }

    public void loadFromFile(BufferedReader br) throws IOException {
        String line;
        line = br.readLine();

        while (line != null) {
            String[] strs = line.split("@");

            String multipolygonStr = strs[0];
            String code = strs[1];

            MultiPolygon mp = new MultiPolygon(multipolygonStr);
            if (table.containsKey(code)) {
                table.get(code).add(mp);
            } else {
                List<MultiPolygon> list = new LinkedList<>();
                list.add(mp);
                table.put(code, list);
            }
            line = br.readLine();
        }
    }
}

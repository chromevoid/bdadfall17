import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MultiPolygon {
    List<Polygon> polygonList;

    public MultiPolygon(List<Polygon> polygonList) {
        this.polygonList = polygonList;
    }

    public MultiPolygon(String text) {
        List<String> polygonTexts = new LinkedList<>();

        // ProcessText
        Pattern pattern = Pattern.compile("\\((\\(\\(.*?\\)\\))\\)");
        Matcher matcher = pattern.matcher(text);

        if (matcher.find()) {
            String polygonsText = matcher.group(1);
            Pattern polygonPattern = Pattern.compile("\\(\\(.*?\\)\\)");
            Matcher polygonMatcher = polygonPattern.matcher(polygonsText);
            while (polygonMatcher.find()) {
                polygonTexts.add(polygonMatcher.group(0));
            }
        }

        // Construction
        polygonList = new LinkedList<>();
        for (String polygonText : polygonTexts) {
            polygonList.add(new Polygon(polygonText));
        }
    }

    public boolean isPointIn(Point p) {
        for (Polygon polygon : polygonList) {
            if (polygon.isPointIn(p)) {
                return true;
            }
        }
        return false;
    }

}

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Polygon {
    public final static List<LineString> EMPTY = new ArrayList<>();

    LineString exterior;
    List<LineString> interiors;

    public Polygon(LineString exterior, List<LineString> interiors) {
        this.exterior = exterior;
        this.interiors = interiors;
    }

    public Polygon(LineString exterior) {
        this(exterior, EMPTY);
    }

    public Polygon(String text) {
        String exteriorText = null;
        List<String> interiorTexts = new LinkedList<>();

        // Process Text
        String linestringInPolygonPattern = "\\(\\((.*?)\\)((,\\s+\\(.*?\\))*)\\)";
        Pattern patternOfPolygon = Pattern.compile(linestringInPolygonPattern);
        Matcher matcherOfPolygon = patternOfPolygon.matcher(text);

        if (!matcherOfPolygon.matches()) {
            System.err.println("Error when processing " + text);
        }

        matcherOfPolygon.reset();

        while (matcherOfPolygon.find()) {
            exteriorText = matcherOfPolygon.group(1);

            if (matcherOfPolygon.groupCount() > 2) {
                String interiorText = matcherOfPolygon.group(2);

                Pattern patternOfLineString = Pattern.compile("\\((.*?)\\)");
                Matcher matcherOfLineString = patternOfLineString.matcher(interiorText);
                while (matcherOfLineString.find()) {
                    interiorTexts.add(matcherOfLineString.group(1));
                }
            }
        }

        // Construction
        exterior = new LineString(exteriorText);
        interiors = new LinkedList<>();
        for (String interiorText : interiorTexts) {
            interiors.add(new LineString(interiorText));
        }
    }

    boolean isPointIn(Point p) {
        if (exterior.isPointIn(p)) {
            for (LineString interior : interiors) {
                if (interior.isPointIn(p)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}

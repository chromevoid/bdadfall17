import java.util.ArrayList;
import java.util.List;

public class LineString {
    public final static LineString EMPTY = new LineString(new ArrayList<Point>());

    List<Point> points;

    public LineString(List<Point> points) {
        this.points = points;
    }

    public LineString(double[] coordinatePairs) {
        points = new ArrayList<>(coordinatePairs.length / 2);
        for (int i = 0; i < coordinatePairs.length; i += 2) {
            double x = coordinatePairs[i];
            double y = coordinatePairs[i + 1];
            points.add(new Point(x, y));
        }
    }

    public LineString(String text) {
        this(getCoordinatePairs(text));
    }

    private static double[] getCoordinatePairs(String text) {
        String[] pointsText = text.split(",");
        double[] coordinatePairs = new double[pointsText.length * 2];
        for (int i = 0; i < pointsText.length; i++) {
            String pointText = pointsText[i].trim(); // remove the space
            String xText = pointText.split(" ")[0];
            String yText = pointText.split(" ")[1];
            coordinatePairs[2 * i] = Double.parseDouble(xText);
            coordinatePairs[2 * i + 1] = Double.parseDouble(yText);
        }
        return coordinatePairs;
    }
    
    boolean isPointIn(Point p) {
        boolean isOdd = false;
        for (int i = 0; i < points.size(); i++) {
            Point p1 = points.get(i);
            Point p2 = points.get((i + 1) % (points.size()));
            double x = p.x, y = p.y, x1 = p1.x, y1 = p1.y, x2 = p2.x, y2 = p2.y;

            if (((y1 <= y && y < y2) || (y2 <= y && y < y1)) &&
                    (x < x1 + (y - y1) / (y2 - y1) * (x2 - x1))) {
                isOdd = !isOdd;
            }
        }
        return isOdd;
    }
}

package org.cisiondata.modules.jstorm.spout.kafka.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ObjectReader {

    @SuppressWarnings({ "serial", "rawtypes" })
	public static List<String> getStrings(final Object o) {
        if (o == null) {
            return new ArrayList<String>();
        } else if (o instanceof String) {
            return new ArrayList<String>() {{ add((String) o); }};
        } else if (o instanceof Collection) {
            List<String> answer = new ArrayList<String>();
            for (Object v : (Collection) o) {
                answer.add(v.toString());
            }
            return answer;
        } else {
            throw new IllegalArgumentException("Don't know how to convert to string list");
        }
    }

    public static String getString(Object o) {
        if (null == o) {
            throw new IllegalArgumentException("Don't know how to convert null to String");
        }
        return o.toString();
    }

    public static Integer getInt(Object o) {
        Integer result = getInt(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to int");
        }
        return result;
    }

    public static Integer getInt(Object o, Integer defaultValue) {
        if (null == o) {
            return defaultValue;
        }

        if (o instanceof Integer ||
                o instanceof Short ||
                o instanceof Byte) {
            return ((Number) o).intValue();
        } else if (o instanceof Long) {
            final long l = (Long) o;
            if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
                return (int) l;
            }
        } else if (o instanceof String) {
            return Integer.parseInt((String) o);
        }

        throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
    }

    public static Double getDouble(Object o) {
        Double result = getDouble(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to double");
        }
        return result;
    }

    public static Double getDouble(Object o, Double defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to double");
        }
    }

    public static boolean getBoolean(Object o, boolean defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Boolean) {
            return (Boolean) o;
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to boolean");
        }
    }

    public static String getString(Object o, String defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof String) {
            return (String) o;
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to String");
        }
    }
}

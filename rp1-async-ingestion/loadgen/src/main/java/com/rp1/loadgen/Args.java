package com.rp1.loadgen;

import java.util.HashMap;
import java.util.Map;

/** Minimal --key value / --flag parser. */
final class Args {
    private final Map<String, String> values = new HashMap<>();

    Args(String[] argv, int from) {
        for (int i = from; i < argv.length; i++) {
            String a = argv[i];
            if (!a.startsWith("--")) throw new IllegalArgumentException("unexpected argument: " + a);
            String k = a.substring(2);
            int eq = k.indexOf('=');
            if (eq >= 0) values.put(k.substring(0, eq), k.substring(eq + 1));
            else if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) values.put(k, argv[++i]);
            else values.put(k, "true");
        }
    }

    String str(String k, String def) { return values.getOrDefault(k, def); }
    String required(String k) {
        String v = values.get(k);
        if (v == null) throw new IllegalArgumentException("missing --" + k);
        return v;
    }
    int integer(String k, int def) { return values.containsKey(k) ? Integer.parseInt(values.get(k)) : def; }
    long lng(String k, long def) { return values.containsKey(k) ? Long.parseLong(values.get(k)) : def; }
    double dbl(String k, double def) { return values.containsKey(k) ? Double.parseDouble(values.get(k)) : def; }
    boolean flag(String k) { return Boolean.parseBoolean(values.getOrDefault(k, "false")); }
}

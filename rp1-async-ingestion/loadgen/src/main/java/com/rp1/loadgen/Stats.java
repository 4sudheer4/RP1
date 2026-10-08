package com.rp1.loadgen;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

final class Stats {
    private Stats() {}

    /** Nearest-rank percentiles over raw samples (no histogram approximation). */
    static Map<String, Object> percentiles(Collection<Long> samples) {
        long[] v = samples.stream().mapToLong(Long::longValue).sorted().toArray();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count", v.length);
        if (v.length == 0) return m;
        m.put("p50", rank(v, 0.50));
        m.put("p90", rank(v, 0.90));
        m.put("p99", rank(v, 0.99));
        m.put("p999", rank(v, 0.999));
        m.put("max", v[v.length - 1]);
        return m;
    }

    private static long rank(long[] sorted, double q) {
        int idx = (int) Math.ceil(q * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
    }

    /** after - before for every Prometheus sample line whose name starts with prefix (counters and _count/_sum). */
    static Map<String, Double> promDelta(String before, String after, String prefix) {
        Map<String, Double> b = parse(before, prefix);
        Map<String, Double> out = new TreeMap<>();
        parse(after, prefix).forEach((k, v) -> {
            if (k.contains("_bucket")) return;
            double d = v - b.getOrDefault(k, 0.0);
            if (d != 0) out.put(k, d);
        });
        return out;
    }

    static Map<String, Double> parse(String text, String prefix) {
        Map<String, Double> m = new TreeMap<>();
        for (String line : text.split("\n")) {
            if (line.startsWith("#") || !line.startsWith(prefix)) continue;
            int sp = line.lastIndexOf(' ');
            if (sp < 0) continue;
            try {
                m.put(line.substring(0, sp), Double.parseDouble(line.substring(sp + 1)));
            } catch (NumberFormatException ignored) {
                // NaN/Inf or exemplars: skip
            }
        }
        return m;
    }
}

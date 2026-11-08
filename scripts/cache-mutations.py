#!/usr/bin/env python3
"""Mutate the real VCL, require an assertion failure, and restore exact source bytes."""
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "src/templates/policy.vcl.j2"
MATRIX = "freshness_privacy_and_parser_matrix"
MUTANTS = [
    ("freshness", MATRIX, 'set beresp.ttl = std.duration(beresp.http.X-Stalewise-Fresh + "s", 0s) -\n            std.duration(beresp.http.Age + "s", 0s);', 'set beresp.ttl = 0s;'),
    ("shared-precedence", MATRIX, 'if (beresp.http.X-Stalewise-Control ~ "(?i)(^|,)\\s*s-maxage\\s*=")', 'if (false)'),
    ("zero-freshness", MATRIX, 'if (beresp.ttl <= 0s && beresp.grace <= 0s)', 'if (beresp.ttl <= 0s && beresp.grace <= 0s) { set beresp.ttl = 60s; } if (false)'),
    ("privacy", MATRIX, '(private|no-store|no-cache)', '(not-a-directive)'),
    ("set-cookie", MATRIX, 'if (beresp.http.Set-Cookie)', 'if (false)'),
    ("vary-star", MATRIX, 'if (beresp.http.Vary ~ "(^|,)\\s*\\*\\s*(,|$)")', 'if (false)'),
    ("age", MATRIX, 'std.duration(beresp.http.Age + "s", 0s);', '0s;'),
    ("surrogate-precedence", MATRIX, 'if (beresp.http.Surrogate-Control) {\n        set beresp.http.X-Stalewise-Control', 'if (false) {\n        set beresp.http.X-Stalewise-Control'),
    ("surrogate-stripping", MATRIX, 'unset resp.http.Surrogate-Control;', 'unset resp.http.X-Mutant;'),
    ("parser", MATRIX, 'call stalewise_validate_control;', ''),
    ("case", MATRIX, '(?i)', ''),
    ("expires", MATRIX, 'elsif (beresp.http.Surrogate-Control || !beresp.http.Expires)', 'else'),
    ("default-zero", MATRIX, 'elsif (beresp.http.Surrogate-Control || !beresp.http.Expires) {\n        call stalewise_uncacheable;', 'elsif (beresp.http.Surrogate-Control || !beresp.http.Expires) {\n        if (!beresp.http.X-Stalewise-Control) { set beresp.ttl = 60s; } else { call stalewise_uncacheable; }'),
    ("cookie-request", "request_credentials", 'req.http.Authorization || req.http.Cookie', 'req.http.Authorization'),
    ("authorization-request", "request_credentials", 'req.http.Authorization || req.http.Cookie', 'req.http.Cookie'),
    ("body-methods", "request_credentials", 'if (req.method != "GET" && req.method != "HEAD")', 'if (false)'),
    ("vary-custom", "vary_partitions", 'std.collect(beresp.http.Vary);', 'unset beresp.http.Vary;'),
    ("conditional", "conditional_head", 'call stalewise_policy_request;', 'unset req.http.If-None-Match; unset req.http.If-Modified-Since; call stalewise_policy_request;'),
    ("head", "conditional_head", 'call stalewise_policy_request;', 'if (req.method == "HEAD") { return (pass); } call stalewise_policy_request;'),
    ("range", "conditional_head", 'call stalewise_policy_request;', 'unset req.http.Range; call stalewise_policy_request;'),
    ("keep", "keep_revalidates", 'set beresp.keep = {{ p.keep_seconds }}s;', 'set beresp.keep = 0s;'),
    ("statuses", "native_cacheable_statuses", 'call stalewise_cacheable_status;', ''),
    ("expiry", "ttl_expires", 'std.duration(beresp.http.Age + "s", 0s);', 'std.duration(beresp.http.Age + "s", 0s) + 100s;'),
    ("hit-for-miss", "hit_for_miss", 'set beresp.ttl = {{ uncacheable_ttl_seconds }}s;', 'set beresp.ttl = 0s;'),
    ("ban", "tag_ban", '" && obj.http.X-Stalewise-Tags ~ ," + req.http.X-Stalewise-Tag + ","', '" && obj.http.X-Stalewise-Tags ~ ,not-a-tag,"'),
    ("purge", "tag_ban", 'return (purge);', 'return (synth(200));'),
    ("small-object-limit", "payload_sizes", 'std.integer(beresp.http.X-Stalewise-Max, {{ max_object_size }})', '1048576'),
    ("large-object-limit", "payload_sizes", 'std.integer(beresp.http.X-Stalewise-Max, {{ max_object_size }})', '314572800'),
    ("chunked-storage", "chunked_objects", 'call stalewise_privacy;', 'if (!beresp.http.Content-Length) { call stalewise_uncacheable; } call stalewise_privacy;'),
    ("stream-storage", "event_streams", 'if (beresp.http.Content-Type ~ "(?i)^\\s*text/event-stream")', 'if (false)'),
    ("progressive-delivery", "event_streams", 'set beresp.do_stream = true;', 'set beresp.do_stream = false;'),
    ("gzip-identity", "vary_accept_encoding", 'call stalewise_policy_request;', 'set req.http.Accept-Encoding = "identity"; call stalewise_policy_request;'),
    ("swr-window", "stale_while", 'if (obj.ttl + std.duration(obj.http.X-Stalewise-Swr + "s", 0s) > 0s)', 'if (false)'),
    ("background-error", "stale_while", '(bereq.is_bgfetch || bereq.http.X-Stalewise-Refetch) && beresp.status >= 500', 'false'),
    ("sie-window", "stale_if_error", 'set beresp.http.X-Stalewise-Sie = regsub(', 'set beresp.http.X-Stalewise-Sie = "0"; set beresp.http.X-Mutant = regsub('),
    ("sie-deadline", "stale_if_error", 'set beresp.grace = std.duration(beresp.http.X-Stalewise-Swr + "s", 0s);', 'set beresp.grace = 60s;'),
]


def main():
    os.chdir(ROOT)
    os.environ.setdefault("STALEWISE_IMAGE", "stalewise:tests")
    original = SOURCE.read_text()
    output = ROOT / ".tools/mutations"
    output.mkdir(parents=True, exist_ok=True)
    survivors = []
    try:
        for name, test, old, new in MUTANTS:
            if len(sys.argv) > 1 and name not in sys.argv[1:]:
                continue
            assert old in original, name
            mutated = original.replace(old, new)
            if name == "surrogate-stripping":
                mutated = mutated.replace("unset beresp.http.Surrogate-Control;", "")
            SOURCE.write_text(mutated)
            command = ["cargo", "test", "--test", "cache_semantics", test,
                       "--", "--ignored", "--nocapture", "--test-threads=1"]
            result = subprocess.run(command, capture_output=True, text=True, timeout=180)
            log = result.stdout + result.stderr
            (output / f"{name}.log").write_text(log)
            red = result.returncode != 0 and "panicked at" in log and "test result: FAILED" in log
            print(f"{name}: {'RED' if red else 'SURVIVED/INVALID'}", flush=True)
            if not red:
                survivors.append(name)
            SOURCE.write_text(original)
    finally:
        SOURCE.write_text(original)
    if survivors:
        raise SystemExit("Surviving/invalid mutants: " + ", ".join(survivors))


if __name__ == "__main__":
    main()

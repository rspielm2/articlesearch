import os
import time
import json
import uuid
import csv
import io
import re
import threading
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from flask import Flask, request, jsonify, Response, render_template
import redis

app = Flask(__name__)

SITEMAP_URL = "https://datasite.my.site.com/datasiteassist/s/sitemap-topicarticle-1.xml"
BASE_API = "{base}/services/data/v58.0/support/knowledgeArticles/{url_name}"
JOB_TTL = 7200  # seconds — jobs expire from Redis after 2 hours

_redis = redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


# ── Redis helpers ──────────────────────────────────────────────────────────────

def _job_key(job_id: str) -> str:
    return f"job:{job_id}"


def _get_job(job_id: str):
    data = _redis.get(_job_key(job_id))
    return json.loads(data) if data else None


def _set_job(job_id: str, job: dict):
    _redis.set(_job_key(job_id), json.dumps(job), ex=JOB_TTL)


# ── HTML stripping ─────────────────────────────────────────────────────────────

class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts: list = []

    def handle_data(self, data: str):
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts)


def _strip_html(html: str) -> str:
    s = _HTMLStripper()
    s.feed(html)
    return s.get_text()


# ── Sitemap & article fetching ─────────────────────────────────────────────────

def _fetch_sitemap():
    """Fetch and parse the sitemap. Returns (base_url, [(url_name, full_url), ...])."""
    req = urllib.request.Request(SITEMAP_URL, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            xml_content = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        raise RuntimeError(f"Could not fetch sitemap: {e}")

    urls = []
    base_url = ""
    try:
        root = ET.fromstring(xml_content)
        ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
        for url_elem in root.iter(f"{ns}url"):
            loc = url_elem.find(f"{ns}loc")
            if loc is not None and loc.text:
                full_url = loc.text.strip()
                url_name = full_url.rstrip("/").split("/")[-1]
                if not base_url and "/s/article/" in full_url:
                    base_url = full_url.split("/s/article/")[0]
                urls.append((url_name, full_url))
    except ET.ParseError as e:
        raise RuntimeError(f"Could not parse sitemap: {e}")

    if not urls:
        raise RuntimeError("No URLs found in the sitemap.")
    if not base_url:
        raise RuntimeError("Could not determine API base URL from sitemap.")

    return base_url, urls


def _fetch_article_text(base_url: str, url_name: str):
    api_url = BASE_API.format(
        base=base_url,
        url_name=urllib.parse.quote(url_name, safe="-"),
    )
    req = urllib.request.Request(
        api_url,
        headers={
            "Accept": "application/json",
            "Accept-Language": "en-US",
            "User-Agent": "Mozilla/5.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None

    parts = []
    for item in data.get("layoutItems", []):
        value = item.get("value") or ""
        if item.get("type") == "RICH_TEXT_AREA":
            value = _strip_html(value)
        parts.append(value)
    parts += [data.get("title", "") or "", data.get("summary", "") or ""]
    return " ".join(parts)


# ── Search worker ──────────────────────────────────────────────────────────────

def _run_search(job_id: str, base_url: str, urls: list, phrase: str, case_sensitive: bool):
    search_term = phrase if case_sensitive else phrase.lower()
    matches = []

    for i, (url_name, full_url) in enumerate(urls):
        # Write progress to Redis on every article
        job = _get_job(job_id)
        if not job:
            return  # job was evicted; nothing to do

        job["current"] = i + 1
        job["current_url"] = url_name
        _set_job(job_id, job)

        text = _fetch_article_text(base_url, url_name)
        if text is not None:
            haystack = text if case_sensitive else text.lower()
            if search_term in haystack:
                matches.append({"URL Name": url_name, "Full URL": full_url})
                job["matches"] = matches
                _set_job(job_id, job)

        time.sleep(0.05)

    # Mark complete
    job = _get_job(job_id)
    if job:
        job["matches"] = matches
        job["done"] = True
        _set_job(job_id, job)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start_search():
    phrase = request.form.get("phrase", "").strip()
    case_sensitive = request.form.get("case_sensitive", "false").lower() == "true"

    if not phrase:
        return jsonify({"error": "Please enter a search phrase."}), 400

    try:
        base_url, urls = _fetch_sitemap()
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 502

    job_id = str(uuid.uuid4())
    _set_job(job_id, {
        "done": False,
        "current": 0,
        "total": len(urls),
        "current_url": "",
        "matches": [],
        "phrase": phrase,
    })

    thread = threading.Thread(
        target=_run_search,
        args=(job_id, base_url, urls, phrase, case_sensitive),
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id, "total": len(urls)})


@app.route("/progress/<job_id>")
def progress(job_id: str):
    def generate():
        while True:
            job = _get_job(job_id)
            if not job:
                yield f"data: {json.dumps({'error': 'Job not found'})}\n\n"
                return

            payload = {
                "current": job["current"],
                "total": job["total"],
                "current_url": job["current_url"],
                "done": job["done"],
                "match_count": len(job["matches"]),
            }
            yield f"data: {json.dumps(payload)}\n\n"

            if payload["done"]:
                return

            time.sleep(0.5)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/results/<job_id>")
def results(job_id: str):
    job = _get_job(job_id)
    if not job or not job["done"]:
        return jsonify({"error": "Job not found or not yet complete."}), 404
    return jsonify({"matches": job["matches"], "phrase": job.get("phrase", "")})


@app.route("/download/<job_id>")
def download(job_id: str):
    job = _get_job(job_id)
    if not job or not job["done"]:
        return jsonify({"error": "Job not found or not yet complete."}), 404

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["URL Name", "Full URL"])
    writer.writeheader()
    writer.writerows(job["matches"])
    output.seek(0)

    safe_phrase = re.sub(r"[^\w\s-]", "_", job.get("phrase", "results")).strip().replace(" ", "_")

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="search_results_{safe_phrase}.csv"'},
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

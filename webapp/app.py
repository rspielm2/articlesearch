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
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, Response, render_template

app = Flask(__name__)

_jobs: dict = {}
_jobs_lock = threading.Lock()

BASE_API = "{base}/services/data/v58.0/support/knowledgeArticles/{url_name}"


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


def _parse_sitemap(xml_content: str):
    """
    Parse a standard sitemap XML file.
    Returns (base_url, [(url_name, full_url), ...]).
    base_url is derived from URLs containing /s/article/.
    """
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
    except ET.ParseError:
        pass

    return base_url, urls


def _cleanup_old_jobs():
    cutoff = datetime.utcnow() - timedelta(hours=2)
    with _jobs_lock:
        stale = [
            jid for jid, job in _jobs.items()
            if job.get("created_at", datetime.utcnow()) < cutoff
        ]
        for jid in stale:
            del _jobs[jid]


def _run_search(job_id: str, base_url: str, urls: list, phrase: str, case_sensitive: bool):
    job = _jobs[job_id]
    search_term = phrase if case_sensitive else phrase.lower()
    matches = []

    for i, (url_name, full_url) in enumerate(urls):
        with _jobs_lock:
            if job.get("cancelled"):
                break
            job["current"] = i + 1
            job["current_url"] = url_name

        text = _fetch_article_text(base_url, url_name)
        if text is not None:
            haystack = text if case_sensitive else text.lower()
            if search_term in haystack:
                matches.append({"URL Name": url_name, "Full URL": full_url})
                with _jobs_lock:
                    job["matches"] = list(matches)

        time.sleep(0.05)

    with _jobs_lock:
        job["matches"] = matches
        job["done"] = True


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start_search():
    _cleanup_old_jobs()

    sitemap_file = request.files.get("sitemap")
    phrase = request.form.get("phrase", "").strip()
    case_sensitive = request.form.get("case_sensitive", "false").lower() == "true"

    if not sitemap_file or not phrase:
        return jsonify({"error": "A sitemap file and search phrase are required."}), 400

    xml_content = sitemap_file.read().decode("utf-8", errors="replace")
    base_url, urls = _parse_sitemap(xml_content)

    if not urls:
        return jsonify({"error": "No URLs were found in the sitemap."}), 400
    if not base_url:
        return jsonify({
            "error": "Could not determine the API base URL. "
                     "Ensure sitemap URLs contain /s/article/."
        }), 400

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "done": False,
            "current": 0,
            "total": len(urls),
            "current_url": "",
            "matches": [],
            "cancelled": False,
            "created_at": datetime.utcnow(),
            "phrase": phrase,
        }

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
            with _jobs_lock:
                job = _jobs.get(job_id)
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
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job or not job["done"]:
        return jsonify({"error": "Job not found or not yet complete."}), 404
    return jsonify({"matches": job["matches"], "phrase": job.get("phrase", "")})


@app.route("/download/<job_id>")
def download(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job or not job["done"]:
        return jsonify({"error": "Job not found or not yet complete."}), 404

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["URL Name", "Full URL"])
    writer.writeheader()
    writer.writerows(job["matches"])
    output.seek(0)

    safe_phrase = re.sub(r"[^\w\s-]", "_", job.get("phrase", "results")).strip().replace(" ", "_")
    filename = f"search_results_{safe_phrase}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

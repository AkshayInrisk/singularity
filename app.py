import io
import os
import uuid
import shutil
import zipfile
import subprocess
from pathlib import Path

from flask import Flask, request, jsonify, send_file

app = Flask(__name__)

APP_DIR = Path(__file__).resolve().parent
BASE_WORKDIR = Path(os.environ.get("WORKING_BASE", "/tmp/singularity_work"))

# You can increase if needed; Cloud Run timeout must also be >= this
PIPELINE_TIMEOUT_SECONDS = int(os.environ.get("PIPELINE_TIMEOUT_SECONDS", "1700"))


@app.get("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "singularity-inrisk",
        "usage": {
            "POST /run": "multipart/form-data with file=<csv>, optional termsheet/pdf fields"
        }
    }), 200


def zip_dir_to_bytes(dir_path: Path) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in dir_path.rglob("*"):
            if p.is_file():
                zf.write(p, arcname=str(p.relative_to(dir_path)))
    buf.seek(0)
    return buf


@app.post("/run")
def run():
    # ---- Validate upload ----
    if "file" not in request.files:
        return jsonify({
            "status": "failed",
            "error": "Upload CSV as multipart/form-data with field name 'file'."
        }), 400

    upload = request.files["file"]
    fname = (upload.filename or "").lower()
    if not fname.endswith(".csv"):
        return jsonify({
            "status": "failed",
            "error": "Uploaded file must be a .csv"
        }), 400

    # Optional flags (keep same behavior you had before)
    termsheet = request.form.get("termsheet", os.environ.get("TERMSHEET", "No"))
    pdf = request.form.get("pdf", os.environ.get("PDF", "No"))

    # ---- Per-request isolated workdir (key for many users) ----
    req_id = str(uuid.uuid4())
    workdir = BASE_WORKDIR / req_id
    input_dir = workdir / "input"
    output_dir = workdir / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # pipeline expects exactly this filename:
    input_csv_path = input_dir / "product_input.csv"
    upload.save(str(input_csv_path))

    env = os.environ.copy()
    env["WORKING_DIR"] = str(workdir)
    env["TERMSHEET"] = str(termsheet)
    env["PDF"] = str(pdf)

    try:
        proc = subprocess.run(
            ["python", "pipeline.py"],
            cwd=str(APP_DIR),
            env=env,
            capture_output=True,
            text=True,
            timeout=PIPELINE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        shutil.rmtree(workdir, ignore_errors=True)
        return jsonify({
            "status": "failed",
            "error": f"Pipeline timed out after {PIPELINE_TIMEOUT_SECONDS}s"
        }), 504
    except Exception as e:
        shutil.rmtree(workdir, ignore_errors=True)
        return jsonify({
            "status": "failed",
            "error": f"Failed to start pipeline: {type(e).__name__}: {e}"
        }), 500

    if proc.returncode != 0:
        shutil.rmtree(workdir, ignore_errors=True)
        return jsonify({
            "status": "failed",
            "returncode": proc.returncode,
            "stderr_tail": (proc.stderr or "")[-4000:],
            "stdout_tail": (proc.stdout or "")[-4000:],
        }), 500

    # ---- Return outputs as download (ZIP) ----
    try:
        # if pipeline produced nothing, fail clearly
        has_any_file = any(p.is_file() for p in output_dir.rglob("*"))
        if not has_any_file:
            shutil.rmtree(workdir, ignore_errors=True)
            return jsonify({
                "status": "failed",
                "error": "Pipeline succeeded but produced no files in output/."
            }), 500

        zip_bytes = zip_dir_to_bytes(output_dir)

        # cleanup after zip is built in memory
        shutil.rmtree(workdir, ignore_errors=True)

        # Download prompt: client saves it (browser usually goes to Downloads automatically)
        return send_file(
            zip_bytes,
            as_attachment=True,
            download_name="singularity_outputs.zip",
            mimetype="application/zip",
        )
    except Exception as e:
        shutil.rmtree(workdir, ignore_errors=True)
        return jsonify({
            "status": "failed",
            "error": f"Failed to package outputs: {type(e).__name__}: {e}"
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), debug=True)

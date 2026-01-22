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

# Cloud Run request timeout must be >= this
PIPELINE_TIMEOUT_SECONDS = int(os.environ.get("PIPELINE_TIMEOUT_SECONDS", "1700"))


@app.get("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "singularity-inrisk",
        "usage": {
            "POST /run": (
                "multipart/form-data with file=<csv> (required), "
                "data_working_zip=<zip> (optional; must contain Data_Working/...), "
                "optional termsheet/pdf fields"
            )
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


def safe_extract_zip_to_dir(zip_path: Path, dest_dir: Path) -> None:
    """
    Extract zip into dest_dir safely (prevents Zip Slip).
    """
    dest_dir_resolved = dest_dir.resolve()
    with zipfile.ZipFile(str(zip_path), "r") as z:
        for member in z.namelist():
            target_path = (dest_dir / member).resolve()
            if not str(target_path).startswith(str(dest_dir_resolved)):
                raise ValueError(f"Unsafe zip entry: {member}")
        z.extractall(str(dest_dir))


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

    try:
        # pipeline expects exactly this filename:
        input_csv_path = input_dir / "product_input.csv"
        upload.save(str(input_csv_path))

        # OPTIONAL: upload cached Data_Working as a zip to skip BigQuery
        if "data_working_zip" in request.files and request.files["data_working_zip"].filename:
            zf_upload = request.files["data_working_zip"]
            zname = (zf_upload.filename or "").lower()

            if not zname.endswith(".zip"):
                shutil.rmtree(workdir, ignore_errors=True)
                return jsonify({
                    "status": "failed",
                    "error": "data_working_zip must be a .zip file"
                }), 400

            zip_path = workdir / "data_working.zip"
            zf_upload.save(str(zip_path))

            # Extract into output_dir so pipeline can find output_dir/Data_Working/*
            safe_extract_zip_to_dir(zip_path, output_dir)

            # Validate expected cache exists
            risk_parquet = output_dir / "Data_Working" / "Risk_Datas.parquet"
            if not risk_parquet.exists():
                shutil.rmtree(workdir, ignore_errors=True)
                return jsonify({
                    "status": "failed",
                    "error": (
                        "Zip extracted, but output/Data_Working/Risk_Datas.parquet was not found. "
                        "Your zip must contain a top-level folder named 'Data_Working/' "
                        "with Risk_Datas.parquet and related cache files inside."
                    )
                }), 400

        env = os.environ.copy()
        env["WORKING_DIR"] = str(workdir)
        env["TERMSHEET"] = str(termsheet)
        env["PDF"] = str(pdf)

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
    except ValueError as e:
        # zip safety / validation errors end up here too
        shutil.rmtree(workdir, ignore_errors=True)
        return jsonify({
            "status": "failed",
            "error": str(e)
        }), 400
    except Exception as e:
        shutil.rmtree(workdir, ignore_errors=True)
        return jsonify({
            "status": "failed",
            "error": f"Failed to run: {type(e).__name__}: {e}"
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

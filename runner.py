import subprocess as sp, sys, pathlib, datetime

PY = sys.executable           # use the current interpreter (handles venvs)
ATLAS = pathlib.Path(__file__).resolve().parent
LOG_DIR = ATLAS / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

def run_and_log(script, log_name):
    print(f">> {log_name}")
    with (LOG_DIR / f"{log_name}.log").open("w", encoding="utf-8") as f:
        f.write(f"Started: {datetime.datetime.now()}\n")
        f.write(f"CWD: {ATLAS}\nCMD: {PY} {script}\n\n")
        res = sp.run([PY, script], cwd=ATLAS, stdout=f, stderr=sp.STDOUT)
        if res.returncode != 0:
            raise SystemExit(f"{log_name} failed with exit code {res.returncode}")

def run_parallel(pairs):
    procs = []
    for script, base in pairs:
        out = (LOG_DIR / f"{base}.out.log").open("w", encoding="utf-8")
        err = (LOG_DIR / f"{base}.err.log").open("w", encoding="utf-8")
        print(f">> starting {script}")
        p = sp.Popen([PY, script], cwd=ATLAS, stdout=out, stderr=err)
        procs.append((p, out, err, script))
    for p, out, err, script in procs:
        code = p.wait()
        out.close(); err.close()
        if code != 0:
            raise SystemExit(f"{script} failed with exit code {code}")

if __name__ == "__main__":
    if not ATLAS.exists():
        raise SystemExit(f"atlas directory not found at {ATLAS}")

    # 1) first
    run_and_log("Final-atlasforlast500.py", "1_final-atlasforlast500")

    # 2) middle pair together
    run_parallel([
        ("ticketnumber.py", "2_ticketnumber"),
        ("1stmessagefetch.py", "3_1stmessagefetch"),
    ])

    # 3) last
    run_and_log("Oldtickets.py", "4_oldtickets")

    print("All steps completed.")

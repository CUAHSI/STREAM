"""Verify the wheel in three environments, outside the source checkout."""

import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="streamwq-install-") as directory:
        work = Path(directory)

        def run(*args: str | Path) -> None:
            subprocess.run(args, cwd=work, check=True)

        # Build through an sdist so stale files in build/ cannot enter the wheel.
        run(sys.executable, "-m", "build", "--no-isolation", "--outdir", work, ROOT)
        (wheel,) = work.glob("*.whl")
        for extra in ("core", "pandas", "geopandas"):
            env = work / extra
            run("uv", "venv", "--python", sys.executable, env)
            python = env / "bin" / "python"
            requirement = str(wheel) if extra == "core" else f"{wheel}[{extra}]"
            run("uv", "pip", "install", "--python", python, requirement)

            def test(filename: str) -> None:
                run(python, "-I", "-m", "unittest", "discover", "-s", ROOT / "tests",
                    "-p", filename)

            test("test_installation.py")
            if extra == "core":
                run(python, "-I", "-c",
                    "from importlib.util import find_spec; "
                    "assert all(find_spec(name) is None for name in ('pandas', 'geopandas'))")
                test("test_imports.py")
            elif extra == "pandas":
                run(python, "-I", "-c",
                    "from importlib.util import find_spec; assert find_spec('geopandas') is None")
                test("test_temporal.py")
            else:
                test("test_watersheds.py")


if __name__ == "__main__":
    main()

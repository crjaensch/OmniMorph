import json
from pathlib import Path
import pytest

base_dir = Path(__file__).parent / "data" / "sample-data"

from omni_morph.data.statistics import get_stats

@pytest.mark.parametrize("subdir,ext", [("csv","csv"),("avro","avro"),("parquet","parquet")])
def test_get_stats_userdata1(subdir, ext):
    data_dir = base_dir / subdir
    csv_file = data_dir / f"userdata1.{ext}"
    expected_file = data_dir / f"userdata1_stats.json"
    expected = json.loads(expected_file.read_text())
    stats = get_stats(csv_file)
    # deep compare stats structure with expected
    for col, exp in expected.items():
        got = stats.get(col)
        assert got is not None, f"Missing column {col}"
        assert got["type"] == exp["type"]
        if exp["type"] == "numeric":
            assert got["count"] == exp["count"]
            # allow float/int comparisons for min and max
            assert pytest.approx(exp["min"]) == got["min"]
            assert pytest.approx(exp["max"]) == got["max"]
            assert pytest.approx(exp["mean"]) == got["mean"]
            if exp.get("median") is None:
                assert got.get("median") is None
            else:
                assert pytest.approx(exp["median"]) == got["median"]
        else:
            assert got["distinct"] == exp["distinct"]
            # normalize values to strings (e.g., Timestamp from parquet)
            got_top = sorted(
                [ {"value": str(item["value"]), "count": item["count"]} for item in got["top5"] ],
                key=lambda x: (x["value"], x["count"])
            )
            exp_top = sorted(exp["top5"], key=lambda x: (x["value"], x["count"]))
            assert got_top == exp_top

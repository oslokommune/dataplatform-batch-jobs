import pandas as pd

from aggregator.parquet_logs_to_agg import countGetRequests


def test_parquet_logs_to_agg():
    d = {
        "dataset_id": ["1234", "5678", "094563", "1234", "5678", "1234"],
        "operation": [
            "REST.PUT.PART",
            "REST.GET.OBJECT",
            "REST.GET.OBJECT",
            "REST.GET.OBJECT",
            "REST.GET.OBJECT",
            "REST.GET.OBJECT",
        ],
    }
    df = pd.DataFrame(data=d)
    result = countGetRequests(df)
    # Rows
    assert len(result) == 3
    # Count
    assert result.loc[result["dataset_id"] == "1234"]["count"].values[0] == 2
    assert result.loc[result["dataset_id"] == "5678"]["count"].values[0] == 2
    assert result.loc[result["dataset_id"] == "094563"]["count"].values[0] == 1

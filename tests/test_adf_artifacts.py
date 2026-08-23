"""Phase 2 ADF artifact contract tests (no live Azure access required).

These tests lock in the REVISED Phase 2 design decisions:

* ADF is ingestion-only: one parameterized Copy pipeline
  ``PL_Ingest_Landing_To_Bronze`` (ForEach over the six known sources).
* Bronze is a FILE-BASED (CSV) raw layer in Phase 2 — no Delta, no
  Databricks, no Data Flows.
* Landing is never modified or deleted by the pipeline.

They validate the JSON in ``adf/`` on disk (not the live factory), so they run
offline and fail if someone reintroduces the superseded Delta/Databricks sink.
"""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ADF = REPO_ROOT / "adf"

# source folder -> landing/bronze file name (Phase 1 verified facts)
EXPECTED_SOURCES = {
    "weather": "weather_observations.csv",
    "harvest": "harvest_transactions.csv",
    "fertilizer": "fertilizer_applications.csv",
    "equipment": "equipment_logs.csv",
    "hr": "hr_attendance.csv",
    "finance": "sap_finance_transactions.csv",
}


def load(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


class TestLinkedServices:
    def test_single_adls_linked_service(self):
        files = sorted(p.name for p in (ADF / "linkedService").glob("*.json"))
        assert files == ["LS_Adls_PlantationSimulator.json"]

    def test_adls_linked_service_is_azureblobfs_url_only(self):
        ls = load(ADF / "linkedService" / "LS_Adls_PlantationSimulator.json")
        assert ls["name"] == "LS_Adls_PlantationSimulator"
        props = ls["properties"]
        assert props["type"] == "AzureBlobFS"
        assert props["typeProperties"] == {
            "url": "https://plantationsimulatorrg.dfs.core.windows.net"
        }
        # No secrets in the repo (AGENTS.md §5 rule 5); auth is the ADF
        # managed identity, added as RBAC (Storage Blob Data Contributor).
        assert "accountKey" not in props["typeProperties"]
        assert "sasToken" not in props["typeProperties"]
        assert "sasUri" not in props["typeProperties"]

    def test_no_databricks_linked_service(self):
        # Phase 2 does not use Databricks (revised approved design).
        for path in (ADF / "linkedService").glob("*.json"):
            assert load(path)["properties"]["type"] != "AzureDatabricksDeltaLake"


class TestDatasets:
    def test_exactly_two_datasets(self):
        files = sorted(p.name for p in (ADF / "dataset").glob("*.json"))
        assert files == ["DS_Bronze_Sink.json", "DS_Landing_Source.json"]

    def test_landing_source_dataset(self):
        ds = load(ADF / "dataset" / "DS_Landing_Source.json")
        assert ds["name"] == "DS_Landing_Source"
        props = ds["properties"]
        assert props["type"] == "DelimitedText"
        assert props["linkedServiceName"]["referenceName"] == "LS_Adls_PlantationSimulator"
        params = props["parameters"]
        assert params["SourceContainer"]["defaultValue"] == "landing"
        assert params["SourceFolder"]["type"] == "String"
        loc = props["typeProperties"]["location"]
        assert loc["type"] == "AzureBlobFSLocation"
        assert loc["fileSystem"] == {"type": "Expression", "value": "@dataset().SourceContainer"}
        assert loc["folderPath"] == {"type": "Expression", "value": "@dataset().SourceFolder"}
        tp = props["typeProperties"]
        assert tp["columnDelimiter"] == ","
        assert tp["firstRowAsHeader"] is True
        assert tp["encodingName"] == "UTF-8"

    def test_bronze_sink_dataset_is_file_based_csv(self):
        ds = load(ADF / "dataset" / "DS_Bronze_Sink.json")
        assert ds["name"] == "DS_Bronze_Sink"
        props = ds["properties"]
        # FILE-BASED Bronze (revised design): DelimitedText, not Delta.
        assert props["type"] == "DelimitedText"
        assert props["linkedServiceName"]["referenceName"] == "LS_Adls_PlantationSimulator"
        params = props["parameters"]
        assert params["SinkContainer"]["defaultValue"] == "bronze"
        assert params["SinkFolder"]["type"] == "String"
        assert params["SinkFileName"]["type"] == "String"
        loc = props["typeProperties"]["location"]
        assert loc["type"] == "AzureBlobFSLocation"
        assert loc["fileSystem"] == {"type": "Expression", "value": "@dataset().SinkContainer"}
        assert loc["folderPath"] == {"type": "Expression", "value": "@dataset().SinkFolder"}
        assert loc["fileName"] == {"type": "Expression", "value": "@dataset().SinkFileName"}
        tp = props["typeProperties"]
        assert tp["columnDelimiter"] == ","
        assert tp["firstRowAsHeader"] is True
        assert tp["encodingName"] == "UTF-8"

    def test_no_delta_dataset(self):
        for path in (ADF / "dataset").glob("*.json"):
            ds = load(path)
            assert ds["properties"]["type"] != "AzureDatabricksDeltaLakeDataset"


class TestPipeline:
    @classmethod
    def setup_class(cls):
        cls.pipe = load(ADF / "pipeline" / "PL_Ingest_Landing_To_Bronze.json")

    def test_single_pipeline_file(self):
        files = sorted(p.name for p in (ADF / "pipeline").glob("*.json"))
        assert files == ["PL_Ingest_Landing_To_Bronze.json"]

    def test_name_and_shape(self):
        assert self.pipe["name"] == "PL_Ingest_Landing_To_Bronze"
        activities = self.pipe["properties"]["activities"]
        assert [a["type"] for a in activities] == ["ForEach"]
        for_each = activities[0]
        assert for_each["name"] == "ForEach_Landing_Source"
        # Sequential for deterministic, easily-verified runs.
        assert for_each["typeProperties"]["isSequential"] is True
        assert for_each["typeProperties"]["items"] == {
            "type": "Expression",
            "value": "@pipeline().parameters.SourceItems",
        }
        copies = for_each["typeProperties"]["activities"]
        assert [c["name"] for c in copies] == ["Copy_Landing_To_Bronze"]
        assert all(c["type"] == "Copy" for c in copies)

    def test_copy_is_landing_csv_to_bronze_csv(self):
        copy = self.pipe["properties"]["activities"][0]["typeProperties"]["activities"][0]
        src = copy["typeProperties"]["source"]
        assert src["type"] == "DelimitedTextSource"
        assert src["storeSettings"]["type"] == "AzureBlobFSReadSettings"
        assert src["storeSettings"]["wildcardFileName"] == "@item().sourceFile"
        # Landing files are NEVER deleted or modified by the pipeline.
        assert src["storeSettings"]["deleteFilesAfterCompletion"] is False

        sink = copy["typeProperties"]["sink"]
        assert sink["type"] == "DelimitedTextSink"
        assert sink["storeSettings"]["type"] == "AzureBlobFSWriteSettings"
        # Overwrite => deterministic reruns (bronze/<source>/<file>.csv always
        # equals exactly one copy of the Landing file; no unbounded growth).
        assert sink["storeSettings"]["copyBehavior"] == "Overwrite"
        assert copy["typeProperties"]["enableStaging"] is False
        # NOTE: "quoteAllText": false is NOT valid for Copy activity sinks
        # (real ADF error DelimitedTextInvalidSettings, run
        # 99a9bbda-9e76-11f1-b171-86283166b020). Guard against reintroducing it.
        write_settings = sink.get("formatSettings", {})
        assert "quoteAllText" not in write_settings

    def test_no_forbidden_sinks(self):
        def walk(node):
            if isinstance(node, dict):
                if node.get("type") in (
                    "AzureDatabricksDeltaLakeSink",
                    "AzureDatabricksDeltaLakeSource",
                ):
                    raise AssertionError(f"Forbidden connector type in pipeline: {node['type']}")
                if node.get("type") == "ExecuteDataFlow" or "DataFlow" in str(node.get("type", "")):
                    raise AssertionError(f"Forbidden activity type in pipeline: {node.get('type')}")
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(self.pipe)

    def test_dataset_references(self):
        copy = self.pipe["properties"]["activities"][0]["typeProperties"]["activities"][0]
        inputs = copy["inputs"]
        assert len(inputs) == 1
        assert inputs[0]["referenceName"] == "DS_Landing_Source"
        assert inputs[0]["parameters"]["SourceContainer"] == "@pipeline().parameters.SourceContainer"
        assert inputs[0]["parameters"]["SourceFolder"] == "@item().sourceFolder"
        outputs = copy["outputs"]
        assert len(outputs) == 1
        assert outputs[0]["referenceName"] == "DS_Bronze_Sink"
        assert outputs[0]["parameters"]["SinkContainer"] == "@pipeline().parameters.SinkContainer"
        assert outputs[0]["parameters"]["SinkFolder"] == "@item().sourceFolder"
        assert outputs[0]["parameters"]["SinkFileName"] == "@item().sourceFile"

    def test_default_source_items_cover_the_six_sources(self):
        items = self.pipe["properties"]["parameters"]["SourceItems"]["defaultValue"]
        assert len(items) == 6
        for item in items:
            assert set(item) == {"sourceFolder", "sourceFile"}
            assert EXPECTED_SOURCES[item["sourceFolder"]] == item["sourceFile"]
        assert {i["sourceFolder"] for i in items} == set(EXPECTED_SOURCES)

    def test_container_defaults(self):
        params = self.pipe["properties"]["parameters"]
        assert params["SourceContainer"]["defaultValue"] == "landing"
        assert params["SinkContainer"]["defaultValue"] == "bronze"
        assert params["SourceContainer"]["type"] == "String"
        assert params["SinkContainer"]["type"] == "String"


class TestNoSupersededDatabricksPhase2Artifacts:
    def test_phase2_delta_ddl_removed(self):
        # The superseded one-time Databricks Delta DDL is gone (Phase 2 is
        # file-based Bronze; Delta/Spark is Phase 3 scope).
        assert not (REPO_ROOT / "databricks" / "sql" / "01_create_bronze_tables.sql").exists()

    def test_no_adf_json_references_databricks(self):
        for path in ADF.rglob("*.json"):
            text = path.read_text(encoding="utf-8")
            assert "Databricks" not in text, f"{path} still references Databricks"
            assert "plantation_bronze" not in text, f"{path} still references the superseded Delta DB"

    def test_verify_script_targets_csv_bronze(self):
        text = (ADF / "scripts" / "verify_bronze.py").read_text(encoding="utf-8")
        assert "bronze/<source>/<file>.csv" in text
        assert "_delta_log" in text  # guards that Phase 2 creates no Delta tables


class TestVerifyScriptContract:
    """Lock the verify script's expected facts to the Phase 1 ground truth."""

    def test_expected_sources_match_phase1(self):
        # Import the verify script as a module and compare its constants.
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "verify_bronze", ADF / "scripts" / "verify_bronze.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        assert module.EXPECTED_SOURCES == {
            "weather": ("weather_observations.csv", 6_483, 506_283),
            "harvest": ("harvest_transactions.csv", 9_112, 971_406),
            "fertilizer": ("fertilizer_applications.csv", 9_000, 1_523_343),
            "equipment": ("equipment_logs.csv", 10_000, 1_566_167),
            "hr": ("hr_attendance.csv", 2_000, 326_323),
            "finance": ("sap_finance_transactions.csv", 12_000, 1_974_121),
        }
        assert module.EXPECTED_TOTAL_ROWS == 48_595
        assert module.LANDING_CONTAINER == "landing"
        assert module.BRONZE_CONTAINER == "bronze"
        assert set(module.DOWNSTREAM_CONTAINERS) == {
            "silver", "gold", "live-bronze", "live-silver", "checkpoints", "incoming"
        }

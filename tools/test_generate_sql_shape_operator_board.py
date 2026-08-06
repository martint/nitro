import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("generate_sql_shape_operator_board.py")
SPEC = importlib.util.spec_from_file_location("sql_shape_board", MODULE_PATH)
BOARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BOARD)


class SqlShapeOperatorBoardTest(unittest.TestCase):
    def test_parses_stage_aware_rich_metrics(self):
        text = "\n".join([
            "prefix operator_cpu,nitro,3/TrinoNitroAggregationOperator@17,2,1.000,2.000,3.000,10,20,4,5.000",
            "prefix nitro,tpch-parquet-sf10,q12,1,1,1,1,1,7.000,7.000,1,1,1,1,1",
        ])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.log"
            path.write_text(text)
            rows = BOARD.parse_log(path, "tpch-parquet-sf10")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stage"], "3")
        self.assertEqual(rows[0]["plan_node"], "17")
        self.assertEqual(rows[0]["family"], "aggregation")
        self.assertEqual(rows[0]["output_positions"], 4)
        self.assertEqual(rows[0]["finish_cpu_ms"], 3)

    def test_averages_multiple_measurement_summaries(self):
        text = "\n".join([
            "operator_cpu,trino,HashAggregationOperator,4,4.000,6.000,2.000",
            "trino,clickbench,q01,2,1,1,1,1,8.000,9.000,1,1,1",
        ])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.log"
            path.write_text(text)
            rows = BOARD.parse_log(path, "clickbench")
        self.assertEqual(rows[0]["drivers"], 2)
        self.assertEqual(rows[0]["add_input_cpu_ms"], 2)
        self.assertEqual(rows[0]["get_output_cpu_ms"], 3)
        self.assertEqual(rows[0]["physical_input_positions"], 0)

    def test_parses_interleaved_engine_summaries(self):
        text = "\n".join([
            "operator_cpu,trino,HashAggregationOperator,2,4.000,0.000,0.000",
            "operator_cpu,nitro,TrinoNitroAggregationOperator,2,2.000,0.000,0.000",
            "trino,tpch-parquet-sf10,q01,2,1,1,1,1,4.000,4.000,1,1,1",
            "nitro,tpch-parquet-sf10,q01,2,1,1,1,1,2.000,2.000,1,1,1",
        ])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.log"
            path.write_text(text)
            rows = BOARD.parse_log(path, "tpch-parquet-sf10")
        self.assertEqual([(row["engine"], row["add_input_cpu_ms"]) for row in rows], [
            ("trino", 2),
            ("nitro", 1),
        ])

    def test_loads_combined_suite_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            for suite_name, benchmark_name in BOARD.SUITES.items():
                (directory / f"{suite_name}.log").write_text("\n".join([
                    "operator_cpu,trino,HashAggregationOperator,1,2.000,0.000,0.000",
                    "operator_cpu,nitro,TrinoNitroAggregationOperator,1,1.000,0.000,0.000",
                    f"trino,{benchmark_name},q01,1,1,1,1,1,2.000,2.000,1,1,1",
                    f"nitro,{benchmark_name},q01,1,1,1,1,1,1.000,1.000,1,1,1",
                ]))
            rows = BOARD.load(directory)
        self.assertEqual(len(rows), 6)
        self.assertEqual({(row["suite"], row["engine"]) for row in rows}, {
            (suite, engine)
            for suite in BOARD.SUITES
            for engine in BOARD.ENGINES
        })

    def test_markdown_reconciles_operator_and_query_cpu(self):
        rows = []
        for suite in BOARD.SUITES:
            for engine, operator_cpu, query_cpu in (("nitro", 6.0, 6.0), ("trino", 10.0, 10.0)):
                rows.append({
                    "suite": suite,
                    "query": "q01",
                    "engine": engine,
                    "query_cpu_mean_ms": query_cpu,
                    "stage": "1",
                    "family": "aggregation",
                    "add_input_cpu_ms": operator_cpu,
                    "get_output_cpu_ms": 0.0,
                    "finish_cpu_ms": 0.0,
                })
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "board.md"
            BOARD.write_markdown(path, rows)
            text = path.read_text()
        self.assertIn("| tpch | 6.0 | 10.0 | 0.600 | 100.000% | 100.000% |", text)
        self.assertIn("| tpch | aggregation | 6.0 | 10.0 | 0.600 | -4.0 |", text)
        self.assertNotIn("| tpch | q01 |", text)


if __name__ == "__main__":
    unittest.main()

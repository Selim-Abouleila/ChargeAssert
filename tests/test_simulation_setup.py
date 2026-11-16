"""Check app access setup fails before mutation and grants only result reads."""
import unittest
from unittest.mock import Mock

from notebooks.setup_simulation_app import grant_result_access


PRINCIPAL = "12345678-1234-5678-1234-567812345678"


class SimulationSetupTests(unittest.TestCase):
    def test_missing_table_stops_before_any_grant(self):
        spark = Mock()
        spark.catalog.tableExists.side_effect = [True, False]
        with self.assertRaisesRegex(ValueError, "run_generated_pipeline"):
            grant_result_access(spark, "workspace", PRINCIPAL)
        spark.sql.assert_not_called()

    def test_only_two_result_tables_receive_select(self):
        spark = Mock()
        spark.catalog.tableExists.return_value = True
        grant_result_access(spark, "team-catalog", PRINCIPAL)
        queries = [call.args[0] for call in spark.sql.call_args_list]
        self.assertEqual(len(queries), 5)
        self.assertEqual([q.split(" ON ")[0] for q in queries],
                         ["GRANT USE CATALOG", "GRANT USE SCHEMA", "GRANT SELECT",
                          "GRANT USE SCHEMA", "GRANT SELECT"])
        reads = [q for q in queries if q.startswith("GRANT SELECT")]
        self.assertIn("`team-catalog`.`chargeassert_dev_bronze`.`generated_job_execution`", reads[0])
        self.assertIn("`team-catalog`.`chargeassert_dev_gold`.`generated_execution_verdict`", reads[1])
        self.assertTrue(all(q.endswith(f"TO `{PRINCIPAL}`") for q in queries))
        self.assertNotIn("ALL PRIVILEGES", " ".join(queries))

    def test_untrusted_identifiers_cannot_become_sql(self):
        for catalog, principal in [("workspace`; DROP TABLE anything", PRINCIPAL),
                                   ("workspace", "someone` WITH GRANT OPTION"),
                                   ("workspace", ""), (None, PRINCIPAL)]:
            with self.subTest(catalog=catalog, principal=principal):
                spark = Mock()
                with self.assertRaises(ValueError):
                    grant_result_access(spark, catalog, principal)
                spark.sql.assert_not_called()
                spark.catalog.tableExists.assert_not_called()


if __name__ == "__main__":
    unittest.main()

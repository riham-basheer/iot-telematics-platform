import os
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class TestGoldLakehouseAggregations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark Session for testing
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Test_Gold_Aggregations") \
            .getOrCreate()
        
        # Load the Gold dataset
        cls.gold_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "gold", "fact_trips_summary"))
        cls.df = cls.spark.read.parquet(cls.gold_path)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_primary_key_uniqueness(self):
        """Assert that trip_id is a unique primary key with no duplicates."""
        total_count = self.df.count()
        distinct_trips = self.df.select("trip_id").distinct().count()
        
        self.assertGreater(total_count, 0, "No trips materialized in the Gold layer!")
        self.assertEqual(total_count, distinct_trips, "Duplicate trip_id detected!")

    def test_null_handling_boundaries(self):
        """Assert that primary keys are never null and physics variables handle nulls properly."""
        null_pks = self.df.filter(F.col("trip_id").isNull() | F.col("veh_id").isNull()).count()
        self.assertEqual(null_pks, 0, "Null primary key fields found (trip_id or veh_id).")
        
        # ICE vehicles do not have high-voltage batteries; energy_consumed_kwh must be coalesced to 0.0
        ice_electric_draw = self.df.filter((F.col("engine_type") == "ICE") & (F.col("energy_consumed_kwh") != 0.0)).count()
        self.assertEqual(ice_electric_draw, 0, "ICE vehicles should have 0.0 energy_consumed_kwh.")

    def test_riemann_sum_metric_invariants(self):
        """Assert that integral Riemann sums do not produce physically impossible negative values."""
        self.assertEqual(self.df.filter(F.col("distance_km") < 0).count(), 0, "distance_km < 0")
        self.assertEqual(self.df.filter(F.col("duration_minutes") <= 0).count(), 0, "duration_minutes <= 0")
        self.assertEqual(self.df.filter(F.col("idling_duration_minutes") < 0).count(), 0, "idling_duration_minutes < 0")
        
    def test_esg_financial_invariants(self):
        """Assert that total cost and carbon emissions are never negative."""
        self.assertEqual(self.df.filter(F.col("total_cost_usd") < 0).count(), 0, "total_cost_usd < 0")
        self.assertEqual(self.df.filter(F.col("co2_emissions_kg") < 0).count(), 0, "co2_emissions_kg < 0")
        self.assertEqual(self.df.filter(F.col("co2_avoided_kg") < 0).count(), 0, "co2_avoided_kg < 0")


    def test_powertrain_taxonomy_distribution(self):
        """Assert that the primary engine types were correctly joined and persisted."""
        powertrains = {row["engine_type"] for row in self.df.select("engine_type").distinct().collect()}
        expected_types = {"ICE", "HEV", "PHEV", "EV"}
        
        # Ensure that at least the foundational powertrains are present
        for ptype in ["ICE", "EV"]:
            self.assertIn(ptype, powertrains, f"Missing core {ptype} powertrain partition.")

if __name__ == "__main__":
    unittest.main()

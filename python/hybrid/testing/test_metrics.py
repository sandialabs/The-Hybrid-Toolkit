"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid.metrics
import unittest


class metrics_tests(unittest.TestCase):
    def setUp(self):
        # two clusters
        self.c1 = [1] * 10 + [2] * 10
        self.c2 = self.c1
        self.c3 = [2] * 10 + [1] * 10
        # all singletons
        self.c4 = range(len(self.c1))
        # all in a single cluster
        self.c5 = [1] * 20
        # four clusters
        self.c6 = [1] * 15 + [3] * 5
        # wrong sized data
        self.c7 = [1] * 10

    def test_nchoosek(self):
        n_data = [10, 11, 12]
        k_data = [2, 3, 4]
        nchoosek_results = [[45, 120, 210], [55, 165, 330], [66, 220, 495]]
        for i, n in enumerate(n_data):
            for j, k in enumerate(k_data):
                self.assertEqual(hybrid.metrics.nchoosek(n, k), nchoosek_results[i][j])

    def test_validate_cluster_data(self):
        self.assertEqual(hybrid.metrics.validate_cluster_data(self.c1, self.c2), len(self.c1))
        self.assertEqual(hybrid.metrics.validate_cluster_data(self.c1, self.c7), 0)

    def test_contigency_table(self):
        self.assertEqual(hybrid.metrics.contingency_table(self.c1, self.c2), [[100.0, 0.0], [0.0, 90.0]])
        self.assertEqual(hybrid.metrics.contingency_table(self.c1, self.c3), [[100.0, 0.0], [0.0, 90.0]])
        self.assertEqual(hybrid.metrics.contingency_table(self.c4, self.c5), [[0.0, 0.0], [190.0, 0.0]])
        self.assertEqual(hybrid.metrics.contingency_table(self.c1, self.c6), [[50.0, 25.0], [50.0, 65.0]])
        self.assertEqual(hybrid.metrics.contingency_table(self.c1, self.c6), [[50.0, 25.0], [50.0, 65.0]])
        self.assertEqual(hybrid.metrics.contingency_table(self.c1, self.c7), None)

    def test_rand_index(self):
        # should be perfect since the clusterings are equal
        self.assertEqual(hybrid.metrics.rand_index(self.c1, self.c2), 1.0)
        # should be perfect since the clusterings are equivalent with swapped labels
        self.assertEqual(hybrid.metrics.rand_index(self.c1, self.c3), 1.0)
        # should be degenerate case between singleton clusters and single cluster
        self.assertEqual(hybrid.metrics.rand_index(self.c4, self.c5), 0.0)
        # some non pathological case
        self.assertAlmostEqual(hybrid.metrics.rand_index(self.c1, self.c6), 0.605263157894737)
        # should be an error (return of 0) since the lengths of the clusterings are not equal
        self.assertEqual(hybrid.metrics.rand_index(self.c1, self.c7), -1)

    def test_jaccard_index(self):
        # should be perfect since the clusterings are equal
        self.assertEqual(hybrid.metrics.jaccard_index(self.c1, self.c2), 1.0)
        # should be perfect since the clusterings are equivalent with swapped labels
        self.assertEqual(hybrid.metrics.jaccard_index(self.c1, self.c3), 1.0)
        # should be degenerate case between singleton clusters and single cluster
        self.assertEqual(hybrid.metrics.jaccard_index(self.c4, self.c5), 0.0)
        # some non pathological case
        self.assertAlmostEqual(hybrid.metrics.jaccard_index(self.c1, self.c6), 0.4642857142857143)
        # should be an error (return of 0) since the lengths of the clusterings are not equal
        self.assertEqual(hybrid.metrics.jaccard_index(self.c1, self.c7), -1)


if __name__ == "__main__":
    unittest.main()

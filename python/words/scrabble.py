"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid
import string


def build_score_table():
    # These are scrabble tile scores for A through Z.
    score = [1, 3, 3, 2, 1, 4, 2, 4, 1, 8, 5, 1, 3, 1, 1, 4, 10, 1, 1, 1, 2, 1, 3, 10, 1, 1, 1, 1, 4, 4, 8, 4, 10]

    # Build a table of the scores, indexed by both lower and uppercase letters.
    table = {}
    for i in range(26):
        table[string.ascii_lowercase[i]] = score[i]
        table[string.ascii_uppercase[i]] = score[i]

    return table


class ScrabbleScore(hybrid.worker.worker.abstract_worker):
    def __init__(self, scoretable=None, **kwargs):
        hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)
        self.set_uses_model(False)

        if scoretable is None:
            scoretable = build_score_table()
        self.scoretable = scoretable

    def process_observation_core(self, doc, **kwargs):
        word = doc.getMetaData("word")
        score = self.score(word)

        self.setMetaData(doc, "score", score)

        print "[scrabble] processing '%s' - score: %d" % (word, score)

        return doc

    def score(self, word):
        # For non-letter characters (e.g., apostrophes), just default to 0
        # score.
        return sum(self.scoretable.get(letter, 0) for letter in word)


if __name__ == "__main__":
    # Get a DB handle.
    db = hybrid.db.mongodb()

    # Create a worker and a module to compute scrabble scores.
    worker = ScrabbleScore(build_score_table(), name="scrabble")
    manager = hybrid.manager.manager(workers=[worker],
                                     query="test",
                                     input_tag_list=[],
                                     output_tag_list=["tag_scrabble"],
                                     input_db=db,
                                     output_db=db)

    manager.run()

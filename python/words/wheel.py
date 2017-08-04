"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid


class WheelOfFortune(hybrid.worker.worker.abstract_worker):
    def __init__(self, **kwargs):
        hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)

    def process_observation_core(self, doc, **kwargs):
        word = doc.getMetaData("word")

        vowel_count = doc.getMetaData("vowels:count")
        vowel_cost = 250.0 * vowel_count
        self.addMetaData(doc, "vowel_cost", vowel_cost)

        print "[wheel of fortune] processing '%s' - vowel count: %d, vowel cost: $%d" % (word, vowel_count, vowel_cost)

        return doc


if __name__ == "__main__":
    db = hybrid.db.mongodb()

    worker = WheelOfFortune(name="wheel of fortune")
    manager = hybrid.manager.manager(workers=[worker],
                                     query="test",
                                     input_tag_list=["tag_vowel_attributes"],
                                     output_tag_list=["tag_wheel_of_fortune"],
                                     input_db=db,
                                     output_db=db)

    manager.run()

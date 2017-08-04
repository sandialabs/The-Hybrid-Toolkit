"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid


class VowelCounter(hybrid.worker.worker.abstract_worker):
    def __init__(self, **kwargs):
        hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)
        self.set_uses_model(False)

        VowelCounter.vowels = "aeiouAEIOU"

    def process_observation_core(self, doc, **kwargs):
        word = doc.getMetaData("word")

        vowel_count = VowelCounter.count_vowels(word)
        vowel_fraction = float(vowel_count) / float(len(word))

        self.addMetaData(doc, "count", vowel_count)
        self.addMetaData(doc, "fraction", vowel_fraction)

        print "[vowels] processing '%s'" % (word)

        return doc

    @staticmethod
    def count_vowels(word):
        return len(filter(lambda x: x in VowelCounter.vowels, word))

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        if not (jsontype == "vowels.VowelCounter"):
            return None

        return VowelCounter(**json_data)


if __name__ == "__main__":
    db = hybrid.db.mongodb()

    worker = VowelCounter(name="vowels")
    manager = hybrid.manager.manager(workers=[worker],
                                     query="test",
                                     input_tag_list=[],
                                     output_tag_list=["tag_vowel_attributes"],
                                     input_db=db,
                                     output_db=db)

    manager.run()

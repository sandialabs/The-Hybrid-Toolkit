"""
Created on May 2, 2013

@author: wldavis


This processor takes a list of text fields, however I believe it may overwrite the term vector if more than one field is sent per processor.
How should it handle multiple fields? Concatenation of the term vector? Multiple term instances might necessitate more intricate approaches...
Perhaps it should store them under different term vectors..
"""
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""

import hybrid
import nltk
import sys
import datetime
import worker
import BeautifulSoup


class plaintextProcessor(worker.abstract_worker):
    def __init__(self, **kwargs):
        if kwargs.get("default_name") is None:
            kwargs["default_name"] = "plaintextProcessor"

        worker.abstract_worker.__init__(self, **kwargs)
        ''' Create instance of the plain text worker
            Input:
                kwargs: various plain text worker parameters
        '''
        english_words = kwargs.get("english_words")
        stop_words = kwargs.get("stop_words")

        text_fields = []
        binary_text_fields = []
        if kwargs.has_key("text_fields"):
            text_fields = kwargs.get("text_fields")
        if kwargs.has_key("binary_text_fields"):
            binary_text_fields = kwargs.get("binary_text_fields")

        term_vector_name = kwargs.get("term_vector_name")

        if term_vector_name == None:
            term_vector_name = "term_vector"
        # if text_fields == None:
        #            text_fields = ["content"]

        self._english_words = english_words
        self._stop_words = stop_words
        self._text_fields = text_fields
        self._binary_text_fields = binary_text_fields
        self._term_vector_name = term_vector_name
        self._type = "plaintextProcessor"

        self._version = "19.07.2003"
        self._data_version = "19.07.2003"

    # Various text handling functions
    @staticmethod
    def exclude_word(word, stop_words):
        return len(word) < 4 or len(word) > 12 or (word.lower() in stop_words)

    @staticmethod
    def extractFromText(content, english_words, stop_words, logger):

        doc_info = {"doc_length": 0, "word_length": 0, "vocab_diverse": 0, "term_vector": []}

        # Tokenize the  text
        sentences = nltk.sent_tokenize(content.strip())
        sentence_tokens = [nltk.word_tokenize(sent) for sent in sentences]
        cased_words = reduce(lambda x, y: x + y, sentence_tokens)
        words = []
        for word in cased_words:
            word = word.lower()
            if (word in english_words) and not plaintextProcessor.exclude_word(word, stop_words):
                words.append(word)

        # Put 'good' words into the Frequency Dictionary
        fdist = nltk.FreqDist()
        for w in words:
            fdist.inc(w)

            # Sanity check
            #        if len(fdist) == 0:
            #            return doc_info

        # Generate feature list
        term_vector = []
        unique_words = fdist.keys()
        for w in unique_words[:100]:  # Limit to the top 100
            feature_info = {"feature": w, "count": fdist[w]}
            term_vector.append(feature_info)

        if (not term_vector) or (len(term_vector) == 0):
            logger.info("No plain text available: embedding \"no_plain_text\" feature.")
            term_vector = [{"feature": "no_plain_text", "count": 1}]
            #         logger.warning("No parseable text in this document:", doc.getDataBlobUUID())

        # Collocation stuff
        num_collocations = 5
        flat_tokens = reduce(lambda x, y: x + y, sentence_tokens)
        finder = nltk.BigramCollocationFinder.from_words(flat_tokens, 2)
        finder.apply_word_filter(lambda (w): len(w) < 3 or len(w) > 10)
        finder.apply_freq_filter(2)
        bigram_measures = nltk.BigramAssocMeasures()
        try:
            collocations = finder.nbest(bigram_measures.likelihood_ratio, num_collocations)
        except:
            e = sys.exc_info()[1]
            logger.warning("Collocation error len flat_tokens=", len(flat_tokens), "error:", e)
            collocations = []
        # collocations = finder.nbest(bigram_measures.raw_freq, 10)

        # Add collocations to feature list
        collocations_list = []
        collocations_reformat = []
        for c in collocations:
            feature_info = {"feature": c[0] + " " + c[1], "count": 2}
            term_vector.append(feature_info)
            collocations_list.append(feature_info)

        # Now compute stats on the document text
        num_chars = len(content)
        num_words = len(words)
        num_vocab = len(set(words))
        if num_words > 0:
            word_length = num_chars / float(num_words)
            vocab_diverse = num_vocab / float(num_words)  # The inverse of the way this is usually scored
            # So higher is more diverse
        else:
            word_length = 0
            vocab_diverse = 0
        doc_info["collocations"] = collocations_list
        doc_info["term_vector"] = term_vector
        doc_info["doc_length"] = num_words
        doc_info["word_length"] = int(word_length)
        doc_info["vocab_diverse"] = int(vocab_diverse * 100)

        return doc_info

    def process_observation_core(self, doc, **kwargs):
        logger = self._logger

        self.setOverwriteMetaData(True)

        text_fields = self._text_fields
        binary_text_fields = self._binary_text_fields

        if (((text_fields is None) or (text_fields == [])) and (
                    (binary_text_fields is None) or (binary_text_fields == []))):
            logger.info("Unable to process document, no text field names supplied.")
            print "===========adfjhsdfjksahdfjkshdfjkhsdkjflhsadjfkhkl"
            return None, doc

        english_words = self._english_words
        stop_words = self._stop_words

        term_vector_name = self._term_vector_name
        text_extracted = False

        for text_field in text_fields:
            if not (doc.hasMetaData(text_field)):
                logger.info("Skipping because", text_field, "is not in document", doc.getDataBlobUUID())
                continue
            else:
                #                print doc
                # Need to generalize here
                plain_text = doc.getMetaData(text_field)

                doc_info = self.extractFromText(plain_text, english_words, stop_words, logger)

                #        self.setMetaData("plain_text",plain_text)
                #        self.setMetaData("english_words",english_words)
                #        self.setMetaData("stop_words",stop_words)

                # Handle to storage for model parameters


                for k, v in doc_info.iteritems():
                    self.addMetaData(doc, k, v)

                # Add various meta data to any existing items in the feature list
                term_vector = self.getMetaData(doc, term_vector_name)

                # Sort the feature list and then place it back into the document
                term_vector.sort(key=lambda i: i["count"], reverse=True)  # Sort on the 'count' tuple item
                self.setMetaData(doc, term_vector_name, term_vector)
                doc.setMetaData(term_vector_name,
                                term_vector)  # TODO FIX ME making this duplicate at the top level so views work
                text_extracted = True

        for binary_text_field in binary_text_fields:
            try:
                plain_text = doc.getBinaryData(binary_text_field)
            except Exception:
                continue

            doc_info = self.extractFromText(plain_text, english_words, stop_words, logger)

            #        self.setMetaData("plain_text",plain_text)
            #        self.setMetaData("english_words",english_words)
            #        self.setMetaData("stop_words",stop_words)

            # Handle to storage for model parameters


            for k, v in doc_info.iteritems():
                self.addMetaData(doc, k, v)

            # Add various meta data to any existing items in the feature list
            term_vector = self.getMetaData(doc, term_vector_name)

            # Sort the feature list and then place it back into the document
            term_vector.sort(key=lambda i: i["count"], reverse=True)  # Sort on the 'count' tuple item
            self.setMetaData(doc, term_vector_name,
                             term_vector)  # This should be happening already in a previous statement
            doc.setMetaData(term_vector_name,
                            term_vector)  # This should be happening already in a previous statement FIXME make this
            # duplicate check
            text_extracted = True

        if not text_extracted:
            self.addMetaData(doc, "plain_text", False)  # Notice this is an add (will not overwrite by default)
        else:
            self.setMetaData(doc, "plain_text", True)  # While this is a set

        return doc, None

        #        doc.setMetaData("feature_version", __feature_extraction_version)


        # Use feature list for summary information
        #        summary = ", ".join([feature["feature"] for feature in term_vector_name[:5]])
        #        doc.setMetaData("summary", hybrid.encoding.convertToUnicode(summary))

        # Save the document back to CouchDB
        #        if (doc._db != None):
        #            doc.store() # Perhaps this shouldn't be in charge of writing, just modifying the doc

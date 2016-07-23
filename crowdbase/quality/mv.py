# -*- coding: utf-8 -*-

class MV:
    def __init__(self, example_to_label):
        self.example_to_label = example_to_label

    def quality_control(self):
        example_to_label_count = {}
        for e, labels in self.example_to_label.iteritems():
            count = {}
            for l in labels:
                count[j] = count.get(j, 0) + 1
            example_to_label_count[e] = count.items()

        example_to_mvlabel = {} # example to final label
        for example, label_count in example_to_label_count.iteritems():
            final_label =  max(label_count.iteritems(), key=operator.itemgetter(1))[0]
            example_to_mvlabel[example] = final_label

        return example_to_mvlabel


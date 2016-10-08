# -*- coding: utf-8 -*-
import abc

class BasePresenter:
    __metaclass__  = abc.ABCMeta

    def set_name(self, name):
        """
            Set the presenter a new name, which will be the name of attached project.
        """
        self.name = name
        return self

    def set_description(self, description):
        """
            Set the presenter a new description, which will be the description of attached project.
        """
        self.description = description
        return self

    def set_template(self, template):
        """
            Set the presenter a new HTML file.
        """
        self.template = template
        return self

    def set_short_name(self, short_name):
        """
            Set the presenter a new short_name, which will be the short_name of attached project.
        """
        self.short_name = short_name
        return self

    def set_question(self, question):
        """
            Set the presenter a question, which will be the question of attached task presented on the HTML file.
        """
        self.question = question
        return self

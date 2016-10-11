# -*- coding: utf-8 -*-
import abc

class BasePresenter:

    """
        BasePresenter is a base class of presenter.
        We derive ImageLabel, ImageCmp and TextCmp three preseter classes from this base class.
        Users can also use it to define own customized presenters.

        >>> customized_presenter = BasePresenter
        >>> customized_presenter.set_name("image Labeling")
        >>> customized_presenter.set_description("Help us to label an image")
        >>> customized_presenter.set_short_name("imagelabel")
        >>> customized_presenter.set_question("Do you see a human face in this picture?")
        >>> customized_presenter.set_template(string_of_HTML)
        >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\  #doctest: +SKIP
        ...               .set_presenter(presenter, map_func)

    """

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

import abc

class BasePresenter:
    __metaclass__  = abc.ABCMeta

    def set_name(self, name):
        self.name = name
        return self

    def set_description(self, description):
        self.description = description
        return self

    def set_template(self, template):
        self.template = template
        return self
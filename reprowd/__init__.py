from pkg_resources import get_distribution

__all__ = ['crowdcontext','presenter']

__version__ = get_distribution('reprowd').version

from reprowd.crowdcontext import *
from reprowd.presenter.base import *
from reprowd.presenter.image import *
from reprowd.presenter.text import *

from crowdbase.crowdcontext import *
from crowdbase.presenter.image import ImageLabel
import pprint

if __name__ == "__main__":

    object_list = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', \
                         'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']

    cc = CrowdContext()

    crowddata = cc.CrowdData(object_list, "flickr") \
                            .set_presenter(ImageLabel(), map_func = lambda obj: {'url_b':obj}) \
                            .publish_task().get_result(False).quality_control("mv")

    pp = pprint.PrettyPrinter(indent=4)
    print "Images:"
    pp.pprint(crowddata.data["object"])

    print "\nTask:"
    pp.pprint(crowddata.data["task"])

    print "\nResult:"
    pp.pprint(crowddata.data["result"])

    print "\nLabels:"
    pp.pprint(crowddata.data["mv"])

from crowdbase.crowdcontext import *
from crowdbase.presenter.image import ImageLabel

if __name__ == "__main__":

    object_list = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', \
                         'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg', \
                         'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']

    cc = CrowdContext()

    crowddata = cc.CrowdData(object_list, "flickr") \
                            .set_presenter(ImageLabel(), map_func = lambda obj: {'url_b':obj}) \
                            .publish_task().get_result(blocking=True).quality_control("mv")


    print "Data:"
    print crowddata.data["object"]
    print

    print "Task:"
    print crowddata.data["task"]
    print

    print "Result:"
    print crowddata.data["result"]

    print "Quality:"
    print crowddata.data["mv"]

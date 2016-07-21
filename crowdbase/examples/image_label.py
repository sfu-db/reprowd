from crowdbase.crowdcontext import *
from crowdbase.presenter.image import ImageLabel

if __name__ == "__main__":

    object_list = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', \
                         'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg', \
                         'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']

    cc = CrowdContext('http://localhost:7000/', '8df67fd6-9c9b-4d32-a6ab-b0b5175aba30')

    crowddata = cc.CrowdData(object_list, cache_table = "flickr") \
                            .map_to_presenter(ImageLabel(), map_func = lambda obj: {'url_b':obj}) \
                            .publish_task().get_result().quality_control(method = CrowdData.EM)


    print "Data:"
    print crowddata.table["object"]
    print

    print "Task:"
    print crowddata.table["task"]
    print

    print "Result:"
    print crowddata.table["result"]

    print "Quality:"
    print crowddata.table["quality_control_result"]

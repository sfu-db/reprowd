from crowdbase.crowdcontext import *
from crowdbase.presenter.image import ImageLabel

if __name__ == "__main__":

    object_list = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', \
                         'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']

    cc = CrowdContext('http://localhost:7000/', '1588f716-d496-4bb2-b107-9f6b200cbfc9')

    crowddata = cc.CrowdData(object_list, cache_table = "flickr") \
                            .map_to_presenter(ImageLabel(), map_func = lambda obj: {'url_b':obj}) \
                            .publish_task().get_result()


    print "Data:"
    print crowddata.table["raw_object"]
    print

    print "Result:"
    print crowddata.table["result"]


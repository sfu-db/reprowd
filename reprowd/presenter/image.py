# -*- coding: utf-8 -*-
from reprowd.presenter.base import *

class ImageLabel (BasePresenter):
    """
        ImageLabel is a presenter class direved from base presenter class. It is attacted with project.
        ImageLabel basically is used for image label tasks.

        In order to load the images correctly, the map function of set_presenter function should be
        consistent with the picture format in Javascript part of template.

        For example,

        >>> map_func = lambda obj: {'url_b':obj}

        this means the key of your image url in task info json is 'url_b', which will be passed to Javascript
        part of template.

        Thus, in pybossa.taskLoaded function of Javascript part. The key 'url_b' should be used to bind a
        src attribute to a img tag.

        >>> img.attr('src', task.info.url_b).css('height', 460);

    """
    def __init__(self):
        """
            Initialize a ImageLabel presenter for a project. Defaultly, question, project name, project shor_name and
            project description are set as empty, "Image Labeling", "imglabel" and "Help us to label an image" respectively.
            The template attribute is a string of HTML file, which is the content of presenter. For writing a new template,
            check http://docs.pybossa.com/en/latest/user/tutorial.html#presenting-the-tasks-to-the-user.

            >>> presenter = ImageLabel();
            >>> presenter.set_name("Do you see a Human face in this picture?")
            >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\  #doctest: +SKIP
            ...               .set_presenter(presenter, map_func)
            >>> crowddata.presenter
            <reprowd.presenter.image.ImageLabel object at 0x...>
        """
        self.question = ""
        self.name = "Image Labeling"
        self.short_name = "imglabel"
        self.description = "Help us to label an image"
        self.template = """
        <!--
    Task DOM for loading the Flickr Images
    It uses the class="skeleton" to identify the elements that belong to the
    task.
-->
<div class="row skeleton"> <!-- Start Skeleton Row-->
    <div class="col-md-6 "><!-- Start of Question and Submission DIV (column) -->
        <h1 id="question"><span id="i18n_question">${question}</span></h1> <!-- The question will be loaded here -->
        <div id="answer"> <!-- Start DIV for the submission buttons -->
            <!-- If the user clicks this button, the saved answer will be value="yes"-->
            <button class="btn btn-success btn-answer" value='Yes'><i class="icon icon-white icon-thumbs-up"></i> <span id="i18n_yes">Yes</span></button>
            <!-- If the user clicks this button, the saved answer will be value="no"-->
            <button class="btn btn-danger btn-answer" value='No'><i class="icon icon-white icon-thumbs-down"></i> No</button>
            <!-- If the user clicks this button, the saved answer will be value="NoPhoto"-->
            <button class="btn btn-answer" value='NoPhoto'><i class="icon icon-exclamation"></i> <span id="i18n_no_photo">No photo</span></button>
            <!-- If the user clicks this button, the saved answer will be value="NotKnown"-->
            <button class="btn btn-answer" value='NotKnown'><i class="icon icon-white icon-question-sign"></i> <span id="i18n_i_dont_know">I don't know</span></button>
        </div><!-- End of DIV for the submission buttons -->
        <!-- Feedback items for the user -->
        <p><span id="i18n_working_task">You are working now on task:</span> <span id="task-id" class="label label-warning">#</span></p>
        <p><span id="i18n_tasks_completed">You have completed:</span> <span id="done" class="label label-info"></span> <span id="i18n_tasks_from">tasks from</span>
        <!-- Progress progress-bar for the user -->
        <span id="total" class="label label-inverse"></span></p>
        <div class="progress progress-striped">
            <div id="progress" rel="tooltip" title="#" class="progress-bar" style="width: 0%;"></div>
        </div>
        <!--
            This project uses Disqus to allow users to provide some feedback.
            The next section includes a button that when a user clicks on it will
            load the comments, if any, for the given task
        -->
        <div id="disqus_show_btn" style="margin-top:5px;">
            <button class="btn btn-primary btn-lg btn-disqus" onclick="loadDisqus()"><i class="icon-comments"></i> <span id="i18n_show_comments">Show comments</span></button>
            <button class="btn btn-lg btn-disqus" onclick="loadDisqus()" style="display:none"><i class="icon-comments"></i> <span id="i18n_hide_comments">Hide comments</span></button>
        </div><!-- End of Disqus Button section -->
        <!-- Disqus thread for the given task -->
        <div id="disqus_thread" style="margin-top:5px;display:none"></div>
    </div><!-- End of Question and Submission DIV (column) -->
    <div class="col-md-6"><!-- Start of Photo DIV (column) -->
        <a id="photo-link" href="#">
            <img id="photo" src="http://i.imgur.com/GeHxzb7.png" style="max-width=100%">
        </a>
    </div><!-- End of Photo DIV (columnt) -->
</div><!-- End of Skeleton Row -->

<script type="text/javascript">
    /* * * CONFIGURATION VARIABLES: EDIT BEFORE PASTING INTO YOUR WEBPAGE * * */

    /* * * DON'T EDIT BELOW THIS LINE * * */
    function loadDisqus() {
    $("#disqus_thread").toggle();
    $(".btn-disqus").toggle();
    var disqus_shortname = 'pybossa'; // required: replace example with your forum shortname
    //var disqus_identifier = taskId;
    var disqus_developer = 1;

    (function() {
        var dsq = document.createElement('script'); dsq.type = 'text/javascript'; dsq.async = true;
        dsq.src = 'http://' + disqus_shortname + '.disqus.com/embed.js';
        (document.getElementsByTagName('head')[0] || document.getElementsByTagName('body')[0]).appendChild(dsq);
    })();
    }

</script>
<noscript>Please enable JavaScript to view the <a href="http://disqus.com/?ref_noscript">comments powered by Disqus.</a></noscript>

<script type="text/javascript">
(function() {
// Default language
var userLocale = "en";
// Translations
var messages = {"en": {
                        "i18n_welldone": "Well done!",
                        "i18n_welldone_text": "Your answer has been saved",
                        "i18n_loading_next_task": "Loading next task...",
                        "i18n_task_completed": "The task has been completed!",
                        "i18n_thanks": "Thanks a lot!",
                        "i18n_congratulations": "Congratulations",
                        "i18n_congratulations_text": "You have participated in all available tasks!",
                        "i18n_yes": "Yes",
                        "i18n_no_photo": "No photo",
                        "i18n_i_dont_know": "I don't know",
                        "i18n_working_task": "You are working now on task:",
                        "i18n_tasks_completed": "You have completed:",
                        "i18n_tasks_from": "tasks from",
                        "i18n_show_comments": "Show comments:",
                        "i18n_hide_comments": "Hide comments:",
                      },
                "es": {
                        "i18n_welldone": "Bien hecho!",
                        "i18n_welldone_text": "Tu respuesta ha sido guardada",
                        "i18n_loading_next_task": "Cargando la siguiente tarea...",
                        "i18n_task_completed": "La tarea ha sido completadas!",
                        "i18n_thanks": "Muchísimas gracias!",
                        "i18n_congratulations": "Enhorabuena",
                        "i18n_congratulations_text": "Has participado en todas las tareas disponibles!",
                        "i18n_yes": "Sí",
                        "i18n_no_photo": "No hay foto",
                        "i18n_i_dont_know": "No lo sé",
                        "i18n_working_task": "Estás trabajando en la tarea:",
                        "i18n_tasks_completed": "Has completado:",
                        "i18n_tasks_from": "tareas de",
                        "i18n_show_comments": "Mostrar comentarios",
                        "i18n_hide_comments": "Ocultar comentarios",
                      },
               };
// Update userLocale with server side information
 $(document).ready(function(){
     userLocale = document.getElementById('PYBOSSA_USER_LOCALE').textContent.trim();

});

function i18n_translate() {
    var ids = Object.keys(messages[userLocale])
    for (i=0; i<ids.length; i++) {
        console.log("Translating: " + ids[i]);
        if (document.getElementById(ids[i])) {
            document.getElementById(ids[i]).innerHTML = messages[userLocale][ids[i]];
        }
    }
}


function loadUserProgress() {
    pybossa.userProgress('rm').done(function(data){
        var pct = Math.round((data.done*100)/data.total);
        $("#progress").css("width", pct.toString() +"%");
        $("#progress").attr("title", pct.toString() + "% completed!");
        $("#progress").tooltip({'placement': 'left'});
        $("#total").text(data.total);
        $("#done").text(data.done);
    });
}

pybossa.taskLoaded(function(task, deferred) {
    if ( !$.isEmptyObject(task) ) {
        // load image from flickr
        var img = $('<img />');
        img.load(function() {
            // continue as soon as the image is loaded
            deferred.resolve(task);
            pybossaNotify("", false, "loading");
        });
        img.attr('src', task.info.url_b).css('height', 460);
        img.addClass('img-thumbnail');
        task.info.image = img;
    }
    else {
        deferred.resolve(task);
    }
});

pybossa.presentTask(function(task, deferred) {
    if ( !$.isEmptyObject(task) ) {
        i18n_translate();
        $('#photo-link').html('').append(task.info.image);
        // $("#photo-link").attr("href", task.info.link);
        $('#task-id').html(task.id);
        $('.btn-answer').off('click').on('click', function(evt) {
            var btn = $(this);
            var answer = btn.attr("value");
            if (typeof answer != 'undefined') {
                pybossa.saveTask(task.id, answer).done(function() {
                    pybossaNotify("Loading picture...", true, "loading");
                    deferred.resolve();
                });
                if ($("#disqus_thread").is(":visible")) {
                    $('#disqus_thread').toggle();
                    $('.btn-disqus').toggle();
                }
            }
            else {
                pybossaNotify("Oops... Something went wrong.", true, "error");
            }
        });
        pybossaNotify("Loading picture...", false, "loading");
    }
    else {
        $(".skeleton").hide();
        pybossaNotify("Loading picture...", false, "loading");
        pybossaNotify("Thanks! You have participated in all available tasks. Enjoy some of your time!", true, "info");
    }
});

 pybossa.run('${short_name}');
 })();
 </script>
        """

class ImageCmp(BasePresenter):
    """
        ImageCmp is a presenter class direved from base presenter class. It is attacted with project.
        ImageCmp basically is used for a pair of images comparing tasks.

        In order to load the images correctly, the map function of set_presenter function should be
        consistent with the picture format in Javascript part of template.

        For example,

        >>> map_func = lambda obj: {'pic1': obj[0], 'pic2': obj[1]}

        this means the keys of your images url in task info json are 'pic1' and 'pic2', which will be
        passed to Javascript part of template.

        Thus, in pybossa.taskLoaded function of Javascript part. The keys 'pic1' and 'pic2' should be
        used to bind src attributes to two img tag2.

        >>> img1.attr('src', task.info.pic1).css('height', 300);
        >>> img2.attr('src', task.info.pic2).css('height', 300);
    """
    def __init__(self):
        """
            Initialize a ImageCmp presenter for a project. Defaultly, question, project name, project shor_name and
            project description are set as empty, "Image Comparison", "imgcmp" and "Help us to compare images" respectively.
            The template attribute is a string of HTML file, which is the content of presenter. For writing a new template,
            check http://docs.pybossa.com/en/latest/user/tutorial.html#presenting-the-tasks-to-the-user.

            >>> presenter = ImageCmp();
            >>> presenter.set_name("Which picture is more beautiful?")
            >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\  #doctest: +SKIP
            ...               .set_presenter(presenter, map_func)
            >>> crowddata.presenter
            <reprowd.presenter.image.ImageCmp object at 0x...>
        """



        self.question = ""
        self.name = "Image Comparison"
        self.short_name = "imgcmp"
        self.description = "Help us to compare images"
        self.template = """
<!--
    Task DOM for loading the Flickr Images
    It uses the class="skeleton" to identify the elements that belong to the
    task.
-->
<div class="row skeleton"> <!-- Start Skeleton Row-->
    <div class="col-md-6 " style="width:100%;text-align:center;margin-left:auto;margin-right:auto"><!-- Start of Question and Submission DIV (column) -->
        <h1 id="question"><span id="i18n_question">${question}</span></h1> <!-- The question will be loaded here -->
        <br>
        <div class="col-md-6" style="width:100%"><!-- Start of Photo DIV (column) -->

            <a class="a-answer" id="photo-link1" href="#" value="left">
              <img id="photo" style="float:left"src="http://i.imgur.com/GeHxzb7.png">
            </a>
            <a style="text-transform:none;text-decoration:none;font-size:50px"><b>OR</a>
            <a class="a-answer" id="photo-link2" href="#" value="right">
              <img id="photo" style="float:right" src="http://i.imgur.com/GeHxzb7.png">
            </a>
        </div><!-- End of Photo DIV (columnt) -->
    </div>
</div><!-- End of Skeleton Row -->

<script type="text/javascript">
    /* * * CONFIGURATION VARIABLES: EDIT BEFORE PASTING INTO YOUR WEBPAGE * * */

    /* * * DON'T EDIT BELOW THIS LINE * * */
    function loadDisqus() {
    $("#disqus_thread").toggle();
    $(".btn-disqus").toggle();
    var disqus_shortname = 'pybossa'; // required: replace example with your forum shortname
    //var disqus_identifier = taskId;
    var disqus_developer = 1;

    (function() {
        var dsq = document.createElement('script'); dsq.type = 'text/javascript'; dsq.async = true;
        dsq.src = 'http://' + disqus_shortname + '.disqus.com/embed.js';
        (document.getElementsByTagName('head')[0] || document.getElementsByTagName('body')[0]).appendChild(dsq);
    })();
    }

</script>
<noscript>Please enable JavaScript to view the <a href="http://disqus.com/?ref_noscript">comments powered by Disqus.</a></noscript>

<script type="text/javascript">
(function() {
// Default language
var userLocale = "en";
// Translations
var messages = {"en": {
                        "i18n_welldone": "Well done!",
                        "i18n_welldone_text": "Your answer has been saved",
                        "i18n_loading_next_task": "Loading next task...",
                        "i18n_task_completed": "The task has been completed!",
                        "i18n_thanks": "Thanks a lot!",
                        "i18n_congratulations": "Congratulations",
                        "i18n_congratulations_text": "You have participated in all available tasks!",
                        "i18n_yes": "Yes",
                        "i18n_no_photo": "No photo",
                        "i18n_i_dont_know": "I don't know",
                        "i18n_working_task": "You are working now on task:",
                        "i18n_tasks_completed": "You have completed:",
                        "i18n_tasks_from": "tasks from",
                        "i18n_show_comments": "Show comments:",
                        "i18n_hide_comments": "Hide comments:",
                      },
                "es": {
                        "i18n_welldone": "Bien hecho!",
                        "i18n_welldone_text": "Tu respuesta ha sido guardada",
                        "i18n_loading_next_task": "Cargando la siguiente tarea...",
                        "i18n_task_completed": "La tarea ha sido completadas!",
                        "i18n_thanks": "Muchísimas gracias!",
                        "i18n_congratulations": "Enhorabuena",
                        "i18n_congratulations_text": "Has participado en todas las tareas disponibles!",
                        "i18n_yes": "Sí",
                        "i18n_no_photo": "No hay foto",
                        "i18n_i_dont_know": "No lo sé",
                        "i18n_working_task": "Estás trabajando en la tarea:",
                        "i18n_tasks_completed": "Has completado:",
                        "i18n_tasks_from": "tareas de",
                        "i18n_show_comments": "Mostrar comentarios",
                        "i18n_hide_comments": "Ocultar comentarios",
                      },
               };
// Update userLocale with server side information
 $(document).ready(function(){
     userLocale = document.getElementById('PYBOSSA_USER_LOCALE').textContent.trim();

});

function i18n_translate() {
    var ids = Object.keys(messages[userLocale])
    for (i=0; i<ids.length; i++) {
        console.log("Translating: " + ids[i]);
        if (document.getElementById(ids[i])) {
            document.getElementById(ids[i]).innerHTML = messages[userLocale][ids[i]];
        }
    }
}


function loadUserProgress() {
    pybossa.userProgress('dd').done(function(data){
        var pct = Math.round((data.done*100)/data.total);
        $("#progress").css("width", pct.toString() +"%");
        $("#progress").attr("title", pct.toString() + "% completed!");
        $("#progress").tooltip({'placement': 'left'});
        $("#total").text(data.total);
        $("#done").text(data.done);
    });
}

pybossa.taskLoaded(function(task, deferred) {
    if ( !$.isEmptyObject(task) ) {
        // load image from flickr
        var img1 = $('<img />');
        var img2 = $('<img />');
        img1.load(function() {
            // continue as soon as the image is loaded
            deferred.resolve(task);
            pybossaNotify("", false, "loading");
        });
        img1.attr('id','pic1');
        img1.attr('src', task.info.pic1).css('height', 300);
        img1.addClass('img-thumbnail');
        img2.load(function() {
            // continue as soon as the image is loaded
            deferred.resolve(task);
            pybossaNotify("", false, "loading");
        });
        img2.attr('id','pic2');
        img2.attr('src', task.info.pic2).css('height', 300);
        img2.addClass('img-thumbnail');
        task.info.image1 = img1;
        task.info.image2 = img2;
    }
    else {
        deferred.resolve(task);
    }
});

function sleep (time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

pybossa.presentTask(function(task, deferred) {
    if (!$.isEmptyObject(task)) {
        // i18n_translate();
        var whole_width = 500;
        $('#photo-link1').html('').append(task.info.image1);
        var image1 = $('#pic1');
        image1.one('load', function(){
            var img1_padding = ((whole_width - image1.width()) / 2).toString() + "px";
            image1.css("background", "#000000");
            image1.css("padding-left", img1_padding);
            image1.css("padding-right", img1_padding);
        }).each(function() {
            if (this.complete) $(this).load();
        });

        $('#photo-link2').html('').append(task.info.image2);
        var image2 = $('#pic2');
        image2.one('load', function(){
            var img2_padding = ((whole_width - image2.width()) / 2).toString() + "px";
            image2.css("background", "#000000");
            image2.css("padding-left", img2_padding);
            image2.css("padding-right", img2_padding);
        }).each(function() {
            if (this.complete) $(this).load();
        });

        // $("#photo-link").attr("href", task.info.link);
        $('#task-id').html(task.id);
        $('.a-answer').off('click').on('click', function(evt) {
            var btn = $(this);
            var answer = btn.attr("value");
            if (typeof answer != 'undefined') {
                pybossa.saveTask(task.id, answer).done(function() {
                    pybossaNotify("Loading picture...", true, "loading");
                    deferred.resolve();
                });
                if ($("#disqus_thread").is(":visible")) {
                    $('#disqus_thread').toggle();
                    $('.btn-disqus').toggle();
                }
                console.log('finish answer');
            }
            else {
                pybossaNotify("Oops... Something went wrong.", true, "error");
            }
        });
        pybossaNotify("Loading picture...", false, "loading");
        url = window.location.href;
        index = url.indexOf(task.id.toString());
        pre_id = task.id;
    }
    else {
            url = window.location.href;
            var url_list = url.split('/');
            if (url_list[url_list.length - 1] == 'newtask') {
                $(".skeleton").hide();
                pybossaNotify("Loading picture...", false, "loading");
                pybossaNotify("Thanks! You have participated in all available tasks. Enjoy some of your time!", true, "info");
                return;
            }
            var new_url = url.substring(0, index) + (pre_id + 1).toString();
            var query_url = url_list[0] + "//" + url_list[2]+ "/api/task/" + (pre_id + 1).toString();
            var i = 0;
            sleep(10000).then(() => {
                $.get(query_url, function() {
                    window.location.href = new_url;
                }).fail(function(){
                    $(".skeleton").hide();
                    pybossaNotify("Loading picture...", false, "loading");
                    pybossaNotify("Thanks! You have participated in all available tasks. Enjoy some of your time!", true, "info");
                });
            });
                }
});
 pybossa.run('${short_name}');
 })();
 </script>
        """

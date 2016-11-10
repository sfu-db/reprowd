# -*- coding: utf-8 -*-

from reprowd.presenter.base import *



class TextCmp (BasePresenter):
    def __init__(self):
        self.question = ""
        self.name = "Text Comparison"
        self.short_name = "textcmp"
        self.description = "Help us to compare texts"
        self.template = """
        <div class="row skeleton"> <!-- Start Skeleton Row-->
    <div class="col-md-6 "><!-- Start of Question and Submission DIV (column) -->
        <h1 id="question"><span id="i18n_question">${question}</span></h1> <!-- The question will be loaded here -->
        <div id="answer"> <!-- Start DIV for the submission buttons -->
            <!-- If the user clicks this button, the saved answer will be value="yes"-->
            <button class="btn btn-success btn-answer" value='Yes'><i class="icon icon-white icon-thumbs-up"></i>Match</button>
            <!-- If the user clicks this button, the saved answer will be value="no"-->
            <button class="btn btn-danger btn-answer" value='No'><i class="icon icon-white icon-thumbs-down"></i> Unmatch</button>
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
        <!-- <a id="photo-link" href="#">
            <img id="photo" src="http://i.imgur.com/GeHxzb7.png" style="max-width=100%">
        </a> -->
        <table id = "maintable" style = "border: 1px solid black;">
          <tr id = "object-list">
            <td>Loading</td>
            <td>Loading</td>
          </tr>
        </table>
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
                        "i18n_question": "Do these two things match?",
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
                        "i18n_question": "¿Ves una cara humana en esta foto?",
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

function processText(a, b) {
    var keyList = [];
    var dica = {}, dicb = {};
    for (var i in a) {
        console.log(a[i]);
        dica[a[i][0]] = a[i][1];
    }
    for (var i in b) {
        dicb[b[i][0]] = b[i][1];
    }

    var lena = a.length;
    var lenb = b.length;
    var index;

    for (index = 0; index < Math.min(lena, lenb); index++) {
        if (keyList.indexOf(a[index][0]) == -1) {
            keyList.push(a[index][0]);
        }

        if (keyList.indexOf(b[index][0]) == -1) {
            keyList.push(b[index][0]);
        }
    }

    if (index < lena - 1) {
        for (var i = index; i < lena; i++) {
            if (keyList.indexOf(a[i][0]) == -1) {
                keyList.push(a[i][0]);
            }
        }
    }

    if (index < lenb - 1) {
        for (var i = index; i < lenb; i++) {
            if (keyList.indexOf(b[i][0]) == -1) {
                keyList.push(b[i][0]);
            }
        }
    }


    for (var i in keyList) {
        if (! (keyList[i] in dica)) {
            dica[keyList[i]] = "";
        }
        if (! (keyList[i] in dicb)) {
            dicb[keyList[i]] = "";
        }
    }
    var info = [dica, dicb, keyList];
    return info;
}

pybossa.taskLoaded(function(task, deferred) {
    if ( !$.isEmptyObject(task) ) {
        // load image from flickr
        deferred.resolve(task);
        console.log(task.info);
        var obj1 = task.info.obj1;
        var obj2 = task.info.obj2;
        var info = processText(obj1,obj2);
        var a = info[0];
        var b = info[1];
        var keyList = info[2];
        var tr = $('<tr>' +
                    '<td>' + '<font size="20">' + task.info.obj1 + '</font></td>' +
                    '<td>' + '<font size="20">' +task.info.obj2 + '</font></td>' +
                    '</tr>');
        var tableString = '<tr>' +
                    '<th><font size="5">Attributes</th><th><font size="5">Record 1</th><th><font size="5">Record 2</th>' +
                    '</tr>';
        for (var i = 0; i < keyList.length; i++) {
            var rowString = '<tr>' +
                        '<td>' + '<font size="5">' + keyList[i] + '</td>' +
                        '<td>' + '<font size="5">' + a[keyList[i]] + '</td>' +
                        '<td>' + '<font size="5">' + b[keyList[i]] + '</td>' +
                        '</tr>'
            tableString = tableString + rowString;
        }
        var table = $(tableString);
        task.info.tr = table;
    }
    else {
        console.log("123");
        deferred.resolve(task);
    }
});

pybossa.presentTask(function(task, deferred) {
    if ( !$.isEmptyObject(task) ) {
        loadUserProgress();
        i18n_translate();
        // $('#photo-link').html('').append(task.info.image);
        $('#maintable').html('').append(task.info.tr);
        $('table,th,td').css('border', '2px solid black');
        $('table').css('border-collapse','separate');
        $('table').css('border-spacing','2px');
        $('table').css('width','550px');
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

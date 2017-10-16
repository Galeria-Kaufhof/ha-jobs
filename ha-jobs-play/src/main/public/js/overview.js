//ha-jobs
$(document).ready(function () {
    window.jobsByType = []
    window.timelines = {}

    var hostname = location.hostname
    $("#title").text("Jobs Controller for " + hostname)

    var jobsEntryTemplateSource = $("#entry-template").html()
    var jobsEntryLatestTemplateSource = $("#latest-entry-template").html()

    var jobTypeTabTemplateSource = $("#jobtype-tab-nav-template").html()
    var jobTypeTabContentTemplateSource = $("#jobtype-tab-content-template").html()

    var alertTemplateSource = $("#alert-template").html()
    var cronTemplateSource =$("#cron-template").html()
    var itemTemplateSource = $("#item-template").html()
    var itemPopoverContentTemplateSource = $("#item-popover-content-template").html()

    Handlebars.registerHelper('if_eq', function (a, b, opts) {
        if (a == b) // Or === depending on your needs
            return opts.fn(this)
        else
            return opts.inverse(this)
    })

    jobsOverviewTemplate = Handlebars.compile(jobsEntryTemplateSource)
    jobTypeTabTemplate = Handlebars.compile(jobTypeTabTemplateSource)
    jobsEntryContentPlaceholderTemplate = Handlebars.compile(jobTypeTabContentTemplateSource)
    jobsEntryLatestTemplate = Handlebars.compile(jobsEntryLatestTemplateSource)
    alertTemplate = Handlebars.compile(alertTemplateSource)
    itemTemplate = Handlebars.compile(itemTemplateSource)
    itemPopoverContentTemplate = Handlebars.compile(itemPopoverContentTemplateSource)
    cronTemplate = Handlebars.compile(cronTemplateSource)

    fetchAllJobs().then(renderOverview)
    fetchJobTypes().then(renderTabs)

    $('#tab-nav-placeholder [href="#Overview"]').on('click', function () {
        window.location.reload()
    })

    $('body').on('click', function (e) {
        $('[data-toggle=popover]').each(function () {
            // hide any open popovers when the anywhere else in the body is clicked
            if (!$(this).is(e.target) && $(this).has(e.target).length === 0 && $('.popover').has(e.target).length === 0) {
                $(this).popover('hide')
            }
        })
    })

    //renderjson settings
    renderjson.set_show_to_level(1) // first level is expanded
        .set_max_string_length(99) // strings longer than 99 characters will be collapsed

})

function fetchAllJobs() {
    return fetchJobTypes().then(function (response) {
        var deferred = response.jobTypes.map(function (jobType) {
            return fetchJobsByType(jobType)
        })
        return $.when.apply(null, deferred).done(function () {
            var responses = Array.prototype.slice.call(arguments)
            var allJobs = responses.map(function (response) {
                var responseData = response[0]
                var cron = responseData.cron
                return responseData.jobs.map(function (job) {
                    job.cron = cron
                    job.jobDuration = calcDuration(job)
                    return job
                })
            })
            window.allJobs = allJobs
        })
    })
}

function calcDuration(job) {
    var startDate = UUIDToDate(job.jobId).getTime()
    var statusDate = new Date(job.jobStatusTs).getTime()
    var durationAsSeconds = moment.duration(statusDate - startDate).asSeconds()
//var durationHR    = durationAsSeconds.humanize()
    return (durationAsSeconds + " seconds")
}

function renderTabs(res) {   //Daten aus dem response body sind in data
    var compiledJobTypeTabHtml = jobTypeTabTemplate(res)
    var compiledJobTypeContentPlaceholderHtml = jobsEntryContentPlaceholderTemplate(res)
    $("#tab-nav-placeholder").append(compiledJobTypeTabHtml)
    $("#tab-content-placeholder").append(compiledJobTypeContentPlaceholderHtml)
    addOnTabClickListener()

    res.jobTypes.forEach(function (jobType) {
        getLatestJobDetailForJobType(jobType).then(renderJobDetail)
    })
}

function renderOverview() {
    function toLatestJobsArray(jobsByType) {
        var latestJob = jobsByType[0]
        return latestJob
    }

    var latestJobsArray = window.allJobs.map(toLatestJobsArray)
    var jobsObj = {}
    jobsObj.jobs = latestJobsArray
    var compiledJobsEntryHtml = jobsOverviewTemplate(jobsObj)
    $("#jobEntry-placeholder").html(compiledJobsEntryHtml)

}

function addTriggerJobEventListener(jobType) {
    document.getElementById('trigger-' + jobType).addEventListener('click', onRetriggerClick.bind(null, jobType), false)
}

function addCancelJobEventListener(jobType) {
    document.getElementById('cancel-' + jobType).addEventListener('click', onCancelClick.bind(null, jobType), false)
}

function addOnTabClickListener() {
    $('[data-toggle="tab"]').on('shown.bs.tab', function (e) {

        var target = $(e.target).attr("href") // activated tab
        var jobType = target.substr(1, target.length)
        updateTimeline(jobType)
    })
}

function fetchJobsByType(jobType) {
    return $.ajax({
        type: "GET",
        dataType: "json",
        url: "/jobs/" + jobType
    })
}

function retriggerJob(jobType) {
    return $.ajax({
        type: "POST",
        dataType: "json",
        url: "/jobs/" + jobType
    })
}

function cancelJob(jobType) {
    return $.ajax({
        type: "DELETE",
        dataType: "json",
        url: "/jobs/" + jobType
    })
}

function onRetriggerClick(jobType) {
    retriggerJob(jobType)
        .then(function(res){
            res.action = 'trigger'
            document.getElementById('trigger-' + jobType).disabled = true;
            refreshJobDetails(res, jobType)
        })
}

function onCancelClick(jobType) {
    cancelJob(jobType)
        .then(function(res){
            res.action = 'cancel'
            document.getElementById('cancel-' + jobType).disabled = true;
            refreshJobDetails(res, jobType)
        })
}

function refreshJobDetails(res, jobType) {
    var result = (res.status === 'OK') ? 'success' : 'error'
    var context = {}
    context.action = res.action
    context.style = result
    context.info = JSON.stringify(res, undefined, 2)
    var compiledAlert = alertTemplate(context)
    $("#jobEntry-placeholder-" + jobType).append(compiledAlert)
    setTimeout(function () {
        getLatestJobDetailForJobType(jobType)
            .then(renderJobDetail)
            .then(function () {
                $("[data-toggle='popover']").popover('hide')
            })
            .then(function () {
                updateTimeline(jobType)
            }).then(function () {
            $(".popover").remove()
        })
    }, 2000)
}


function fetchJobTypes() {
    return $.ajax({
        type: "GET",
        dataType: "json",
        url: "/jobs"
    })
}

function getLatestJobDetailForJobType(jobType) {
    return $.ajax({
        type: "GET",
        dataType: "json",
        url: "/jobs/" + jobType + "/latest"
    })
}

function renderJobDetail(latestJobForType) {
    var jobType = latestJobForType.jobType
    var compiledJobsEntryLatestHtml = jobsEntryLatestTemplate(latestJobForType)
    //var compiledCronHtml = cronTemplate(latestJobForType.cron)
    var latestJobWithCron = getJobsByType(jobType)
    var jobCron = latestJobWithCron [0][0].cron
    var pretty = ""
    var nextRun = ""
    if(jobCron){
        pretty = cronstrue.toString(jobCron)
        console.log(pretty)

    } else {
        pretty = 'no CRON expression set'
    }
    var compiledCronHtml = cronTemplate({
        pretty: pretty,
        cron: jobCron
    })
    $("#jobEntry-placeholder-" + jobType).html(compiledJobsEntryLatestHtml)
    // Pretty print job content and append to dom
    $(".content-"+latestJobForType.jobId).html(renderjson(latestJobForType.content))
    $("#cron-" + jobType).html(compiledCronHtml)
    addTriggerJobEventListener(jobType)
    addCancelJobEventListener(jobType)
    return fetchAllJobs().then(function () {
        return createTimeline(jobType)
    })
}

function UUIDToIntTime(uuid) {
    var uuidArray = uuid.split('-'),
        timeStr = [
            uuidArray[2].substring(1),
            uuidArray[1],
            uuidArray[0]
        ].join('')
    return parseInt(timeStr, 16)
}

function UUIDToDate(uuid) {
    var intTime = UUIDToIntTime(uuid),
        ms = Math.floor((intTime - 122192928000000000 ) / 10000)
    return new Date(ms)
}

function jobToVisDataSet(job) {
    var end = new Date(job.jobStatusTs)
    var begin = new Date(UUIDToDate(job.jobId))
    var res = {
        id: job.jobId,
        start: UUIDToDate(job.jobId),
        content: job,
        editable: false,
        selectable: false,
        className: statusToClass(job.jobState)
    }

    if (end.getTime() - begin.getTime() > 60 * 1000){ // only set end value for jobs that have a runtime of at least one minute
        res.end = job.jobStatusTs
    }

    return res
}

function statusToClass(jobState) {
    switch (jobState) {
        case"FAILED":
            return "bg-danger"
        case"RUNNING":
            return "bg-warning"
        case"FINISHED":
            return "bg-success"
        default:
            return "bg-info"
    }
}

function updateTimeline(jobType) {
    return fetchAllJobs().then(function () {
        var jobsData = getJobsByType(jobType)[0]
        var dataSet = jobsData.map(jobToVisDataSet)
        var items = new vis.DataSet(dataSet)
        var timelineToUpdate = window.timelines[jobType]
        timelineToUpdate.setData(items)

        jobsData.forEach(function (job) {
            var popoverDetails = itemPopoverContentTemplate(job)
            $('[data-jobId="' + job.jobId + '"]').popover({html: true, content: popoverDetails})
        })
    })
}

function getJobsByType(jobType) {
    return window.allJobs.filter(function (jobs) {
        return jobs[0].jobType === jobType
    })
}

function createTimeline(jobType) {
    var jobsData = getJobsByType(jobType)

    var container = document.getElementById('timeline-' + jobType)

// Create a DataSet (allows two way data-binding)
    var dataSet = jobsData[0].map(jobToVisDataSet)

    var items = new vis.DataSet(dataSet)

// Configuration for the Timeline
    var options = {
        min: new Date(Date.now() - (14 *24 * 60 * 60 * 1000)),  // lowest/earliest date visible in timeline (now - 2 weeks in ms)
        max: new Date(Date.now() + 30 + 60 * 60 * 1000),        // highest/latest date visible in timeline  (now + 30 minutes in ms)
        start: new Date(Date.now() - 3 * 60 * 60 * 1000),       // start date of the initial viewport of the timeline (now - 3h in ms)
        end: new Date(Date.now() + 5 * 60 * 1000),              // end date of the initial viewport of the timeline (now +5min in ms)
        zoomMin: 5 * 60 * 1000,                                 // minimum zoom span (5 minutes in ms)
        template: itemTemplate,
        selectable: false,
        showCurrentTime: true,
    }

// Create a Timeline
    var timeline = new vis.Timeline(container, items, options)
    window.timelines[jobType] = timeline
    //timeline.fit()
}
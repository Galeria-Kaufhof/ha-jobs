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

    jobsOverviewTemplate = Handlebars.compile(jobsEntryTemplateSource)
    jobTypeTabTemplate = Handlebars.compile(jobTypeTabTemplateSource)
    jobsEntryContentPlaceholderTemplate = Handlebars.compile(jobTypeTabContentTemplateSource)
    jobsEntryLatestTemplate = Handlebars.compile(jobsEntryLatestTemplateSource)
    alertTemplate = Handlebars.compile(alertTemplateSource)
    itemTemplate = Handlebars.compile(itemTemplateSource)
    itemPopoverContentTemplate = Handlebars.compile(itemPopoverContentTemplateSource)
    cronTemplate = Handlebars.compile(cronTemplateSource)

    render()

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
    renderjson.set_show_to_level('all') // first level is expanded
        .set_max_string_length(99) // strings longer than 99 characters will be collapsed

})

function render() {
    return fetchJobTypes()
        .then(function (jobs) {
            renderTabs(jobs)
            var deferred = jobs.jobTypes.map(function (jobType) {
                return fetchLatestJobByType(jobType)
            })
            $.when.apply(null, deferred).done(function () {
                var latestJobsJSON = deferred.map(function (res) { return res.responseJSON })
                renderOverview(latestJobsJSON)
            })
        })
}

function renderTabs(res) {
    var compiledJobTypeTabHtml = jobTypeTabTemplate(res)
    var compiledJobTypeContentPlaceholderHtml = jobsEntryContentPlaceholderTemplate(res)
    $("#tab-nav-placeholder").append(compiledJobTypeTabHtml)
    $("#tab-content-placeholder").append(compiledJobTypeContentPlaceholderHtml)
    addOnTabClickListener()
}

function renderOverview(latestJobs) {
    var jobsObj = {}
    jobsObj.jobs = latestJobs.map(function (job) {
        job.jobDuration = calcDuration(job)
        job.stateClass = statusToClass(job.jobState)
        job.resultClass = statusToClass(job.jobResult)
        return job
    })
    var compiledJobsEntryHtml = jobsOverviewTemplate(jobsObj)
    $("#jobEntry-placeholder").html(compiledJobsEntryHtml)
}

function renderJobDetail(latestJobForType) {
    var jobType = latestJobForType.jobType
    latestJobForType.stateClass = statusToClass(latestJobForType.jobState)
    latestJobForType.resultClass = statusToClass(latestJobForType.jobResult)
    fetchJobByType(jobType).then(function (job) {
        var compiledJobsEntryLatestHtml = jobsEntryLatestTemplate(latestJobForType)
        //var compiledCronHtml = cronTemplate(latestJobForType.cron)
        var latestJobs = job.jobs
        var jobCron = job.cron
        var pretty = jobCron ? cronstrue.toString(jobCron) : "no CRON expression set"
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
        createTimeline(jobType, latestJobs)
    })
}

function createTimeline(jobType, latestJobs) {
    var container = document.getElementById('timeline-' + jobType)

// Create a DataSet (allows two way data-binding)
    var dataSet = latestJobs.map(jobToVisDataSet)

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

// Add Popover
    latestJobs.forEach(function (job) {
        var popoverDetails = itemPopoverContentTemplate(job)
        $('[data-jobId="' + job.jobId + '"]').popover({html: true, content: popoverDetails})
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
        fetchLatestJobByType(jobType)
            .then(renderJobDetail)
    }, 2000)
}

// EVENTHANDLER

function addOnTabClickListener() {
    $('[data-toggle="tab"]').on('shown.bs.tab', function (e) {
        var target = $(e.target).attr("href") // activated tab
        var jobType = target.substr(1, target.length)
        fetchLatestJobByType(jobType).then(renderJobDetail)
    })
}

function addTriggerJobEventListener(jobType) {
    setupEventListener(jobType, 'trigger', onRetriggerClick)
}

function addCancelJobEventListener(jobType) {
    setupEventListener(jobType, 'cancel', onCancelClick)
}

function onRetriggerClick(jobType) {
    if (confirm('Retriggering ' + jobType + '\n Are you sure?')) {
        retriggerJob(jobType)
            .then(function(res){
                disableButtonsAndUpdateDetails(jobType, res, 'trigger')
            })
    }
}

function onCancelClick(jobType) {
    if (confirm('Canceling ' + jobType + '\n Are you sure?')) {
        cancelJob(jobType)
            .then(function(res){
                disableButtonsAndUpdateDetails(jobType, res, 'cancel')
            })
    }
}

function setupEventListener(jobType, action, fn) {
    document.getElementById(action + '-' + jobType).addEventListener('click', fn.bind(null, jobType), false)
}

function disableButtonsAndUpdateDetails(jobType, res, action) {
    res.action = action
    document.getElementById(action + '-' + jobType).disabled = true;
    refreshJobDetails(res, jobType)
}

// AJAX CALLS

function fetchJobTypes() {
    return ajaxRequst("GET")
}

function fetchLatestJobByType(type) {
    const path = type + "/latest"
    return ajaxRequst("GET", path)
}

function fetchJobByType(type) {
    return ajaxRequst("GET", type)
}

function retriggerJob(type) {
    return ajaxRequst("POST", type)
}

function cancelJob(type) {
    return ajaxRequst("DELETE", type)
}

function ajaxRequst(type, path) {
    const url = path ? "/jobs/" + path : "/jobs"
    return $.ajax({
        type: type,
        dataType: "json",
        url: url
    })
}

// HELPER
function calcDuration(job) {
    var startDate = UUIDToDate(job.jobId).getTime()
    var statusDate = new Date(job.jobStatusTs).getTime()
    var durationAsSeconds = moment.duration(statusDate - startDate).asSeconds()
//var durationHR    = durationAsSeconds.humanize()
    return (durationAsSeconds + " seconds")
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
    job.jobDuration = calcDuration(job)
    job.stateClass = statusToClass(job.jobState)
    job.resultClass = statusToClass(job.jobResult)
    var end = new Date(job.jobStatusTs)
    var begin = new Date(UUIDToDate(job.jobId))
    var res = {
        id: job.jobId,
        start: UUIDToDate(job.jobId),
        content: job,
        editable: false,
        selectable: false,
        className: "bg-" + statusToClass(job.jobState),
    }

    if (end.getTime() - begin.getTime() > 60 * 1000){ // only set end value for jobs that have a runtime of at least one minute
        res.end = job.jobStatusTs
    }

    return res
}

function statusToClass(state) {
    switch (state) {
        case "RUNNING":
        case "PREPARING":
        case "SKIPPED":
            return "info"
        case "FAILED":
        case "WARNING":
        case "PENNDING":
            return "danger"
        case "SUCCESS":
        case "FINISHED":
        case "NO_ACTION_NEEDED":
            return "success"
        case "RUNNING":
        case "CANCELED":
            return "warning"
    }
}

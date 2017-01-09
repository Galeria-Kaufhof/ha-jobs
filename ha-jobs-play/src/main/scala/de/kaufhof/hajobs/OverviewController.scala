package de.kaufhof.hajobs

import play.api.http.ContentTypes
import play.api.mvc.{Action, Controller}
import play.api.routing.{JavaScriptReverseRoute, JavaScriptReverseRouter}

class OverviewController(jobsInfoRoute: JavaScriptReverseRoute) extends Controller {


  def index() = Action {
    Ok.sendResource("overview.html")
  }

  def jobsInfo = Action { implicit request =>
    Ok("[1,2,3]").as(ContentTypes.JAVASCRIPT)
  }

  def jsRoutes = Action { implicit request =>
    Ok(JavaScriptReverseRouter("jsRoute")(jobsInfoRoute)).as(ContentTypes.JAVASCRIPT)
  }
}
package de.kaufhof.hajobs

import play.api.mvc.{Action, Controller}

class OverviewController() extends Controller {


  def index() = Action {
    Ok.sendResource("overview.html")
  }
}
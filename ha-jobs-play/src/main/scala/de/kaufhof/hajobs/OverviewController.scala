package de.kaufhof.hajobs

import javax.inject.Inject
import play.api.mvc.{AbstractController, Action, ControllerComponents}

class OverviewController @Inject() (components: ControllerComponents)
extends AbstractController(components) {


  def index() = Action {
    Ok.sendResource("overview.html")
  }
}
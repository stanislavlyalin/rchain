package coop.rchain.node

import cats.effect.{Async, Resource, Sync, Temporal}
import cats.syntax.all._
import coop.rchain.comm.discovery.NodeDiscovery
import coop.rchain.comm.rp.Connect.{ConnectionsCell, RPConfAsk}
import coop.rchain.node.api.{AdminWebApi, WebApi}
import coop.rchain.node.diagnostics.NewPrometheusReporter
import coop.rchain.node.web.ReportingRoutes.ReportingHttpRoutes
import coop.rchain.node.web.https4s.RouterFix
import coop.rchain.shared.Log
import org.http4s.HttpRoutes
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.implicits._
import org.http4s.server.Server
import org.http4s.server.middleware.CORS

import scala.concurrent.duration.{DurationInt, FiniteDuration}

package object web {
  def corsPolicy = CORS.policy.withAllowCredentials(false)

  def acquireHttpServer[F[_]: Async: RPConfAsk: NodeDiscovery: ConnectionsCell: Log](
      reporting: Boolean,
      host: String = "0.0.0.0",
      httpPort: Int,
      prometheusReporter: NewPrometheusReporter,
      connectionIdleTimeout: FiniteDuration,
      webApi: WebApi[F],
      reportingRoutes: ReportingHttpRoutes[F]
  ): Resource[F, Server] = {
    val reportingRoutesOpt = if (reporting) reportingRoutes else HttpRoutes.empty
    val baseRoutes = Map(
      "/metrics" -> corsPolicy(NewPrometheusReporter.service[F](prometheusReporter)),
      "/version" -> corsPolicy(VersionInfo.service[F]),
      "/status"  -> corsPolicy(StatusInfo.service[F]),
      "/api"     -> corsPolicy(WebApiRoutes.service[F](webApi) <+> reportingRoutesOpt),
      // Web API v1 with OpenAPI schema
      "/api/v1" -> corsPolicy(WebApiRoutesV1.create[F](webApi))
    )
    // Legacy reporting routes
    val extraRoutes =
      if (reporting)
        Map("/reporting" -> corsPolicy(reportingRoutes))
      else
        Map.empty
    val allRoutes = baseRoutes ++ extraRoutes

    BlazeServerBuilder[F]
      .bindHttp(httpPort, host)
      .withHttpApp(RouterFix(allRoutes.toList: _*).orNotFound)
      .withIdleTimeout(connectionIdleTimeout)
      .withResponseHeaderTimeout(connectionIdleTimeout - 1.second)
      .resource
  }

  def acquireAdminHttpServer[F[_]: Async: Log](
      host: String = "0.0.0.0",
      httpPort: Int,
      connectionIdleTimeout: FiniteDuration,
      webApi: WebApi[F],
      adminWebApiRoutes: AdminWebApi[F],
      reportingRoutes: ReportingHttpRoutes[F]
  ): Resource[F, Server] = {
    val baseRoutes = Map(
      "/api" -> corsPolicy(AdminWebApiRoutes.service[F](adminWebApiRoutes) <+> reportingRoutes),
      // Web API v1 (admin) with OpenAPI schema
      "/api/v1" -> corsPolicy(WebApiRoutesV1.createAdmin[F](webApi, adminWebApiRoutes))
    )
    BlazeServerBuilder[F]
      .bindHttp(httpPort, host)
      .withHttpApp(RouterFix(baseRoutes.toList: _*).orNotFound)
      .withResponseHeaderTimeout(connectionIdleTimeout - 1.second)
      .withIdleTimeout(connectionIdleTimeout)
      .resource
  }
}

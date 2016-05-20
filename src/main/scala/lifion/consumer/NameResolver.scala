//package com.lifion.consumer
//
///**
// * Created by dvorkine on 12/7/15.
// */
//
//import javax.ws.rs.QueryParam
//
//import com.ecwid.consul.v1.catalog.model.CatalogService
//import com.ecwid.consul.v1.{ConsistencyMode, QueryParams, ConsulClient}
//
//import scala.collection.mutable.{StringBuilder, ListBuffer}
//import scala.concurrent.ExecutionContext.Implicits.global
//
//import java.net.{Inet6Address, InetAddress, Inet4Address}
//import com.ecwid.consul._
//
//object NameResolver {
//  val DNS_SERVICE = "consul.service.consul"
//  val CONSUL_PORT = 8500
//
//  def getKafkaBrokers(kafkaBrokerRegex:String): String = {
//
//    val client = new ConsulClient(DNS_SERVICE,CONSUL_PORT);
//    //list known datacenters
//    val q=new QueryParams(ConsistencyMode.DEFAULT)
//      val response = client.getCatalogServices(q)
//    val myNodes: ListBuffer[String] = ListBuffer()
//    import scala.collection.JavaConversions._
//
//    response.getValue().foreach { kv =>
//      if ((kafkaBrokerRegex.r findFirstIn kv._1) != None) {
//
//        val catalogServices=client.getCatalogService(kv._1, q).getValue
//
//        catalogServices.iterator().foreach{ x=>
//          myNodes.insert(0, new StringBuilder(x.getServiceAddress).append(":").append(x.getServicePort.toString).toString())
//
//        }
//
//      }
//
//    }
//
//
//    myNodes.toList.mkString(",")
//
//  }
//
//  def inetMatcher()= {
//
//    InetAddress.getByName(DNS_SERVICE) match {
//      case inet: Inet4Address => inet
//      case inet: Inet6Address =>
//        throw new IllegalArgumentException("An IPv6 address was supplied")
//    }
//  }
//
//
//
//}

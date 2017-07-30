package com.hike

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

case class Ab(
  uid: String,
  msisdn: String
)
case class ABResponse(
  stat: String,
  error: String,
  msisdn: String,
  ab: java.util.List[Ab],
  rab: java.util.List[Ab]
)

case class Friendship(
    first: String,
    second: String
)
    